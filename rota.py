import pandas as pd
from fpdf import FPDF
from geopy.geocoders import Nominatim
from geopy.distance import geodesic
import numpy as np
import os
import unicodedata
import json
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
import time
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
from requests.exceptions import RequestException
import requests
from urllib.parse import quote
from datetime import datetime, timedelta
from tqdm import tqdm
import colorama
from colorama import Fore, Style
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Inicializa colorama para Windows
colorama.init()

def print_colorido(texto, cor=Fore.WHITE, estilo=Style.NORMAL):
    """Imprime texto colorido no terminal"""
    print(f"{estilo}{cor}{texto}{Style.RESET_ALL}")

# Função para remover acentos
def remover_acentos(texto):
    return ''.join(c for c in unicodedata.normalize('NFD', texto)
                  if unicodedata.category(c) != 'Mn')

# Cache para geocodificação com timestamp
CACHE_FILE = "geocodificacao_cache.json"
CACHE_EXPIRATION_DAYS = 30  # Cache expira após 30 dias

def carregar_cache():
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
                # Limpar cache expirado
                current_time = datetime.now()
                cache_data = {
                    k: v for k, v in cache_data.items()
                    if datetime.fromisoformat(v['timestamp']) + timedelta(days=CACHE_EXPIRATION_DAYS) > current_time
                }
                return cache_data
        except Exception as e:
            print(f"Erro ao carregar cache: {str(e)}")
            return {}
    return {}

def salvar_cache(cache):
    try:
        with open(CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Erro ao salvar cache: {str(e)}")

def expandir_abreviacoes(endereco):
    """Expande abreviações comuns em endereços"""
    abreviacoes = {
        'Av.': 'Avenida',
        'Av ': 'Avenida',
        'R.': 'Rua',
        'R ': 'Rua',
        'Al.': 'Alameda',
        'Al ': 'Alameda',
        'Tv.': 'Travessa',
        'Tv ': 'Travessa',
        'Est.': 'Estrada',
        'Est ': 'Estrada'
    }
    
    for abrev, completo in abreviacoes.items():
        endereco = endereco.replace(abrev, completo)
    
    return endereco

def geocodificar_endereco(endereco, max_tentativas=3):
    """Geocodifica um endereço usando a API do Nominatim com tratamento de erros melhorado"""
    endereco_sem_acento = remover_acentos(endereco)
    endereco_formatado = f"{endereco_sem_acento}, Brasil"
    url = f"https://nominatim.openstreetmap.org/search?q={quote(endereco_formatado)}&format=json&limit=1"
    
    headers = {
        'User-Agent': 'RotaEntregas/1.0 (https://github.com/juniorchiodi/Planejador-de-Rotas; juninho.junirj@gmail.com) Python/3.x'
    }

    for tentativa in range(max_tentativas):
        try:
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code == 403:
                time.sleep(10)
                continue
                
            response.raise_for_status()
            
            data = response.json()
            if data:
                lat = float(data[0]['lat'])
                lon = float(data[0]['lon'])
                time.sleep(1)  # Reduzido de 2 para 1 segundo
                return {
                    'coords': (lat, lon),
                    'timestamp': datetime.now().isoformat()
                }
            else:
                pass
                
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            if tentativa < max_tentativas - 1:
                tempo_espera = (tentativa + 1) * 5  # Reduzido de 10 para 5 segundos
                time.sleep(tempo_espera)
            continue
        except Exception as e:
            if tentativa < max_tentativas - 1:
                time.sleep(5)
            continue
    
    return None

def calcular_distancia_graphhopper(coords1, coords2):
    """Calcula a distância usando o serviço GraphHopper"""
    try:
        lon1, lat1 = coords1[1], coords1[0]
        lon2, lat2 = coords2[1], coords2[0]
        
        url = f"https://graphhopper.com/api/1/route?point={lat1},{lon1}&point={lat2},{lon2}&vehicle=car&key=YOUR_API_KEY"
        response = requests.get(url, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            if 'paths' in data and data['paths']:
                return data['paths'][0]['distance'] / 1000  # Converte para km
    except Exception:
        pass
    return None

def calcular_distancia_valhalla(coords1, coords2):
    """Calcula a distância usando o serviço Valhalla"""
    try:
        lon1, lat1 = coords1[1], coords1[0]
        lon2, lat2 = coords2[1], coords2[0]
        
        url = f"http://valhalla.openstreetmap.de/route?json={{\"locations\":[{{\"lat\":{lat1},\"lon\":{lon1}}},{{\"lat\":{lat2},\"lon\":{lon2}}}],\"costing\":\"auto\"}}"
        response = requests.get(url, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            if 'trip' in data and 'legs' in data['trip']:
                return data['trip']['legs'][0]['length'] / 1000  # Converte para km
    except Exception:
        pass
    return None

def calcular_distancia_osrm(coords1, coords2, max_tentativas=3):
    """Calcula a distância entre dois pontos usando o OSRM com alta precisão"""
    try:
        # Formata as coordenadas para o formato OSRM
        lon1, lat1 = coords1[1], coords1[0]
        lon2, lat2 = coords2[1], coords2[0]
        
        # URL do serviço OSRM com parâmetros otimizados
        url = f"http://router.project-osrm.org/route/v1/driving/{lon1},{lat1};{lon2},{lat2}?overview=full&alternatives=true&steps=true&annotations=distance"
        
        for tentativa in range(max_tentativas):
            try:
                # Aumenta o timeout para 15 segundos e adiciona retry
                session = requests.Session()
                retry = Retry(total=3, backoff_factor=0.5)
                adapter = HTTPAdapter(max_retries=retry)
                session.mount('http://', adapter)
                session.mount('https://', adapter)
                
                response = session.get(url, timeout=15)
                
                if response.status_code == 200:
                    data = response.json()
                    if data['code'] == 'Ok':
                        # Pega a rota com menor distância entre as alternativas
                        rotas = data['routes']
                        if rotas:
                            # Ordena as rotas por distância
                            rotas_ordenadas = sorted(rotas, key=lambda x: x['distance'])
                            melhor_rota = rotas_ordenadas[0]
                            
                            # Calcula a distância total considerando os passos
                            distancia_total = 0
                            for leg in melhor_rota['legs']:
                                for step in leg['steps']:
                                    distancia_total += step['distance']
                            
                            return distancia_total / 1000  # Converte para km
                            
                elif response.status_code == 429:  # Rate limit
                    print_colorido("⚠️ Rate limit atingido. Aguardando 5 segundos...", Fore.YELLOW)
                    time.sleep(5)
                    continue
                    
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                print_colorido(f"⚠️ Erro de conexão na tentativa {tentativa + 1}: {str(e)}", Fore.RED)
                if tentativa < max_tentativas - 1:
                    tempo_espera = (tentativa + 1) * 5
                    print_colorido(f"   Aguardando {tempo_espera} segundos...", Fore.YELLOW)
                    time.sleep(tempo_espera)
                continue
            except Exception as e:
                print_colorido(f"❌ Erro inesperado: {str(e)}", Fore.RED)
                if tentativa < max_tentativas - 1:
                    print_colorido("   Aguardando 5 segundos...", Fore.YELLOW)
                    time.sleep(5)
                continue
                
    except Exception as e:
        print(f"Erro ao calcular distância OSRM: {str(e)}")
    
    return None

def calcular_distancia_rua(coords1, coords2):
    """Calcula a distância entre dois pontos usando a fórmula de Haversine"""
    try:
        # Converte as coordenadas para o formato correto e valida
        lat1, lon1 = float(coords1[0]), float(coords1[1])
        lat2, lon2 = float(coords2[0]), float(coords2[1])
        
        # Validação das coordenadas
        if not (-90 <= lat1 <= 90) or not (-90 <= lat2 <= 90) or \
           not (-180 <= lon1 <= 180) or not (-180 <= lon2 <= 180):
            print(f"Coordenadas inválidas: ({lat1}, {lon1}) ou ({lat2}, {lon2})")
            return float('inf')
        
        # Calcula a distância usando geodesic
        dist = geodesic((lat1, lon1), (lat2, lon2)).kilometers
        
        # Validação da distância
        if dist > 500:  # Se a distância for maior que 500km, provavelmente está errado
            print(f"Distância suspeita: {dist:.2f}km entre ({lat1}, {lon1}) e ({lat2}, {lon2})")
            return float('inf')
        
        return dist
    except Exception as e:
        print(f"Erro ao calcular distância: {str(e)}")
        return float('inf')

def calcular_distancia_final(coords1, coords2):
    """Calcula a distância final usando OSRM com fallback para cálculo geodésico"""
    # Tenta primeiro o OSRM
    dist_osrm = calcular_distancia_osrm(coords1, coords2)
    if dist_osrm is not None:
        return round(dist_osrm * 1.1, 1)  # Adiciona 10% de margem
    
    # Se OSRM falhar, usa o cálculo geodésico
    dist_geodesica = calcular_distancia_rua(coords1, coords2)
    return round(dist_geodesica * 1.15, 1)  # Adiciona 15% de margem para cálculo geodésico

# Cache para distâncias
DISTANCE_CACHE_FILE = "distance_cache.json"
DISTANCE_CACHE_EXPIRATION_DAYS = 30

def carregar_cache_distancia():
    if os.path.exists(DISTANCE_CACHE_FILE):
        try:
            with open(DISTANCE_CACHE_FILE, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
                # Limpar cache expirado
                current_time = datetime.now()
                cache_data = {
                    k: v for k, v in cache_data.items()
                    if datetime.fromisoformat(v['timestamp']) + timedelta(days=DISTANCE_CACHE_EXPIRATION_DAYS) > current_time
                }
                return cache_data
        except Exception as e:
            print(f"Erro ao carregar cache de distância: {str(e)}")
            return {}
    return {}

def salvar_cache_distancia(cache):
    try:
        with open(DISTANCE_CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Erro ao salvar cache de distância: {str(e)}")

def calcular_distancia_com_cache(coords1, coords2):
    """Calcula a distância entre dois pontos com cache e validação"""
    # Cria uma chave única para o par de coordenadas
    key = f"{coords1[0]},{coords1[1]}_{coords2[0]},{coords2[1]}"
    
    # Carrega o cache
    cache = carregar_cache_distancia()
    
    # Verifica se a distância está em cache
    if key in cache:
        distancia_cache = cache[key]['distance']
        
        # Valida se a distância do cache faz sentido
        distancia_geodesica = calcular_distancia_rua(coords1, coords2)
        
        # Aumenta a tolerância para 100% para evitar recálculos desnecessários
        # Isso é seguro porque o cálculo geodésico é sempre menor que a distância real
        if distancia_geodesica > 0 and distancia_cache > distancia_geodesica * 2:
            print_colorido(f"⚠️ Distância no cache muito maior que a geodésica. Recalculando...", Fore.YELLOW)
            distancia = calcular_distancia_final(coords1, coords2)
        else:
            return distancia_cache
    
    # Calcula a distância usando o novo sistema
    distancia = calcular_distancia_final(coords1, coords2)
    
    # Salva no cache
    cache[key] = {
        'distance': distancia,
        'timestamp': datetime.now().isoformat()
    }
    salvar_cache_distancia(cache)
    
    return distancia

def identificar_outliers(dist_matrix, enderecos_validos, limite_desvio=2):
    """Identifica pontos que estão muito distantes da média"""
    n = len(dist_matrix)
    if n <= 1:
        return [], []

    # Calcula a média e desvio padrão das distâncias
    distancias = []
    for i in range(n):
        for j in range(n):
            if i != j and dist_matrix[i][j] != float('inf'):
                distancias.append(dist_matrix[i][j])
    
    if not distancias:
        return [], []

    media = sum(distancias) / len(distancias)
    desvio = (sum((x - media) ** 2 for x in distancias) / len(distancias)) ** 0.5
    
    # Identifica outliers
    outliers = []
    pontos_principais = []
    
    for i in range(n):
        distancias_ponto = [dist_matrix[i][j] for j in range(n) if i != j and dist_matrix[i][j] != float('inf')]
        if not distancias_ponto:
            continue
            
        media_ponto = sum(distancias_ponto) / len(distancias_ponto)
        if media_ponto > media + (limite_desvio * desvio):
            outliers.append(i)
            print(f"Ponto identificado como outlier: {enderecos_validos[i]} (distância média: {media_ponto:.2f} km)")
        else:
            pontos_principais.append(i)
    
    return pontos_principais, outliers

def encontrar_melhor_rota(dist_matrix, enderecos_validos):
    """Encontra a melhor rota sempre indo para o vizinho mais próximo"""
    n = len(dist_matrix)
    if n <= 1:
        return [0]

    # Função para verificar se a distância é aceitável
    def distancia_aceitavel(dist):
        return dist < 20  # Reduzido para 20km para priorizar pontos muito próximos

    # Função para encontrar o próximo ponto mais próximo com validação
    def encontrar_proximo_ponto(ponto_atual, pontos_nao_visitados):
        # Primeiro tenta encontrar um ponto muito próximo (até 5km)
        pontos_muito_proximos = [p for p in pontos_nao_visitados 
                               if dist_matrix[ponto_atual][p] < 5]
        if pontos_muito_proximos:
            return min(pontos_muito_proximos, key=lambda x: dist_matrix[ponto_atual][x])
        
        # Depois tenta encontrar um ponto próximo (até 10km)
        pontos_proximos = [p for p in pontos_nao_visitados 
                         if dist_matrix[ponto_atual][p] < 10]
        if pontos_proximos:
            return min(pontos_proximos, key=lambda x: dist_matrix[ponto_atual][x])
        
        # Depois tenta encontrar um ponto com distância aceitável (até 20km)
        pontos_validos = [p for p in pontos_nao_visitados 
                         if distancia_aceitavel(dist_matrix[ponto_atual][p])]
        if pontos_validos:
            return min(pontos_validos, key=lambda x: dist_matrix[ponto_atual][x])
        
        # Se não encontrar pontos próximos, usa o mais próximo disponível
        return min(pontos_nao_visitados, key=lambda x: dist_matrix[ponto_atual][x])

    # Começa do ponto de partida (índice 0)
    rota = [0]
    pontos_nao_visitados = set(range(1, n))
    
    # Enquanto houver pontos não visitados
    while pontos_nao_visitados:
        ponto_atual = rota[-1]
        
        # Encontra o próximo ponto mais próximo com validação
        proximo_ponto = encontrar_proximo_ponto(ponto_atual, pontos_nao_visitados)
        
        # Adiciona o ponto à rota
        rota.append(proximo_ponto)
        pontos_nao_visitados.remove(proximo_ponto)
        
        # Mostra a distância para o próximo ponto
        distancia = dist_matrix[ponto_atual][proximo_ponto]
        print(f"De {enderecos_validos[ponto_atual]} para {enderecos_validos[proximo_ponto]}: {distancia:.2f} km")
    
    # Tenta otimizar a rota verificando se há pontos que podem ser reordenados
    rota_otimizada = rota.copy()
    melhorou = True
    
    while melhorou:
        melhorou = False
        for i in range(1, len(rota_otimizada) - 1):
            # Verifica se trocar a ordem de dois pontos melhora a distância total
            dist_atual = (dist_matrix[rota_otimizada[i-1]][rota_otimizada[i]] + 
                         dist_matrix[rota_otimizada[i]][rota_otimizada[i+1]])
            dist_nova = (dist_matrix[rota_otimizada[i-1]][rota_otimizada[i+1]] + 
                        dist_matrix[rota_otimizada[i+1]][rota_otimizada[i]])
            
            if dist_nova < dist_atual:
                # Troca os pontos
                rota_otimizada[i], rota_otimizada[i+1] = rota_otimizada[i+1], rota_otimizada[i]
                melhorou = True
                print(f"Otimização: Reordenando pontos {i} e {i+1}")
    
    # Calcula a distância total da rota otimizada
    distancia_total = 0
    for i in range(len(rota_otimizada) - 1):
        distancia_total += dist_matrix[rota_otimizada[i]][rota_otimizada[i + 1]]
    
    print(f"\nDistância total da rota: {distancia_total:.2f} km")
    return rota_otimizada

# CONFIGURAÇÕES
arquivo_excel = "ENDERECOS-ROTA.xlsx"
nome_coluna_enderecos = "Endereco"
nome_coluna_nomes = "Nome"
ponto_partida = "Rua Floriano Peixoto, 368, Itapuí, SP"

# Solicitar a cidade ao usuário
cidade = input("Digite a cidade das entregas: ").strip()

try:
    print_colorido("\n🚀 Iniciando processamento...", Fore.GREEN, Style.BRIGHT)
    
    # Verificar se o arquivo Excel existe
    if not os.path.exists(arquivo_excel):
        print_colorido(f"❌ Erro: O arquivo {arquivo_excel} não foi encontrado.", Fore.RED)
        exit(1)

    # LER PLANILHA
    print_colorido("\n📊 Lendo planilha...", Fore.CYAN)
    try:
        df = pd.read_excel(arquivo_excel)
        enderecos = df[nome_coluna_enderecos].dropna().tolist()
        nomes = df[nome_coluna_nomes].fillna("").tolist()
        print_colorido(f"✅ Total de endereços encontrados: {len(enderecos)}", Fore.GREEN)
    except Exception as e:
        print_colorido(f"❌ Erro ao ler planilha: {str(e)}", Fore.RED)
        exit(1)
    
    if not enderecos:
        print_colorido("❌ Erro: Nenhum endereço encontrado na planilha.", Fore.RED)
        exit(1)

    # GEOCODIFICAÇÃO
    print_colorido("\n🌍 Iniciando geocodificação...", Fore.CYAN)
    coordenadas = []
    enderecos_validos = []
    enderecos_com_erro = []

    # Primeiro, geocodificar o ponto de partida
    print_colorido(f"\n📍 Processando ponto de partida: {ponto_partida}", Fore.CYAN)
    cache = carregar_cache()
    if ponto_partida in cache:
        print_colorido("✅ Usando coordenadas do cache para ponto de partida", Fore.GREEN)
        coordenadas.append(cache[ponto_partida]['coords'])
        enderecos_validos.append(ponto_partida)
    else:
        try:
            resultado = geocodificar_endereco(ponto_partida)
            if resultado:
                coordenadas.append(resultado['coords'])
                enderecos_validos.append(ponto_partida)
                cache[ponto_partida] = resultado
                salvar_cache(cache)
            else:
                print_colorido(f"❌ Erro: Não foi possível geocodificar o ponto de partida: {ponto_partida}", Fore.RED)
                exit(1)
        except Exception as e:
            print_colorido(f"❌ Erro ao geocodificar ponto de partida: {str(e)}", Fore.RED)
            exit(1)

    # Função para processar endereços em paralelo
    def processar_endereco(endereco):
        cache = carregar_cache()
        if endereco in cache:
            # Não imprime aqui, apenas retorna status
            return endereco, cache[endereco]['coords'], 'cache'
        
        resultado = geocodificar_endereco(endereco)
        if resultado:
            cache[endereco] = resultado
            salvar_cache(cache)
            return endereco, resultado['coords'], 'geocodificado'
        return endereco, None, 'erro'

    # Processar endereços em paralelo com mais workers
    print_colorido("\n🔄 Geocodificando endereços...", Fore.CYAN)
    with ThreadPoolExecutor(max_workers=6) as executor:  # Reduzido para 6 workers para maior estabilidade
        resultados = list(tqdm(executor.map(processar_endereco, enderecos), 
                             total=len(enderecos),
                             desc="Progresso",
                             unit="endereço"))
    
    # Filtrar resultados válidos e coletar erros
    for i, (endereco, coords, status) in enumerate(resultados, 1):
        if coords:
            coordenadas.append(coords)
            enderecos_validos.append(endereco)
        else:
            enderecos_com_erro.append((i, endereco))
        # Imprime o status de cada endereço em ordem
        if status == 'cache':
            print_colorido(f"Usando cache para: {endereco}", Fore.YELLOW)
        elif status == 'geocodificado':
            print_colorido(f"Geocodificado: {endereco}", Fore.GREEN)
        else:
            print_colorido(f"Erro ao geocodificar: {endereco}", Fore.RED)

    if len(enderecos_validos) <= 1:
        print_colorido("❌ Erro: Nenhum endereço foi geocodificado com sucesso além do ponto de partida.", Fore.RED)
        exit(1)

    print_colorido(f"\n✅ Total de endereços geocodificados com sucesso: {len(enderecos_validos)}", Fore.GREEN)
    print_colorido(f"⚠️ Total de endereços com erro: {len(enderecos_com_erro)}", Fore.YELLOW)

    # MATRIZ DE DISTÂNCIA
    print_colorido("\n📏 Calculando matriz de distância...", Fore.CYAN)
    n = len(coordenadas)
    dist_matrix = np.zeros((n, n))

    # Função para calcular distâncias em paralelo
    def calcular_distancia_paralela(args):
        i, j, coords1, coords2 = args
        if i != j:
            dist = calcular_distancia_com_cache(coords1, coords2)
            return i, j, dist
        return i, j, 0

    # Criar lista de argumentos para processamento paralelo
    args_list = []
    for i in range(n):
        for j in range(n):
            args_list.append((i, j, coordenadas[i], coordenadas[j]))

    # Calcular distâncias em paralelo
    with ThreadPoolExecutor(max_workers=6) as executor:  # Reduzido para 6 workers para maior estabilidade
        resultados = list(tqdm(executor.map(calcular_distancia_paralela, args_list),
                             total=len(args_list),
                             desc="Calculando distâncias",
                             unit="par"))

    # Preencher matriz de distância com resultados
    for i, j, dist in resultados:
        dist_matrix[i][j] = dist
        if dist != float('inf'):
            print_colorido(f"   De {enderecos_validos[i]} para {enderecos_validos[j]}: {dist:.2f} km", Fore.WHITE)

    # ENCONTRAR MELHOR ROTA
    print_colorido("\n🗺️ Calculando melhor rota...", Fore.CYAN)
    ordem_rota = encontrar_melhor_rota(dist_matrix, enderecos_validos)
    
    if ordem_rota is None:
        print_colorido("❌ Erro: Não foi possível encontrar uma rota válida", Fore.RED)
        exit(1)
    
    # Verifica se a rota está correta
    if len(ordem_rota) != len(coordenadas):
        print_colorido("❌ Erro: A rota não inclui todos os pontos!", Fore.RED)
        exit(1)

    # Calcula a distância total da rota e as distâncias parciais
    distancia_total = 0
    distancias_parciais = []
    for i in range(len(ordem_rota) - 1):
        dist = dist_matrix[ordem_rota[i]][ordem_rota[i + 1]]
        distancia_total += dist
        distancias_parciais.append(dist)
    print_colorido(f"\n📊 Distância total da rota: {distancia_total:.2f} km", Fore.GREEN)

    enderecos_ordenados = [enderecos_validos[i] for i in ordem_rota]
    nomes_ordenados = [nomes[enderecos.index(endereco)] if endereco in enderecos else "" for endereco in enderecos_ordenados]
    links = [f"https://www.google.com/maps/search/?api=1&query={remover_acentos(e).replace(' ', '+')}" for e in enderecos_ordenados]

    # GERAR PDF
    print_colorido("\n📄 Gerando PDF...", Fore.CYAN)
    pdf = FPDF()
    pdf.add_page()
    
    # Adicionar logo
    if os.path.exists("./assets/logo.png"):
        # Posiciona o logo no canto superior direito
        pdf.image("./assets/logo.png", x=170, y=10, w=31.5)
    
    # Configurar fonte e título
    pdf.set_font("Arial", "B", 16)
    
    # Obter data atual no formato brasileiro
    data_atual = datetime.now().strftime("%Y/%m/%d")
    
    # Criar título com cidade e data
    titulo = f"{data_atual} - Rota de Entregas - {cidade}"
    # Definir o nome do arquivo PDF baseado no título, removendo acentos e substituindo espaços e barras
    nome_arquivo = remover_acentos(titulo).replace(" ", "_").replace("/", "-")
    
    # Criar pasta ROTAS-GERADAS se não existir
    pasta_rotas = "ROTAS-GERADAS"
    if not os.path.exists(pasta_rotas):
        os.makedirs(pasta_rotas)
    
    # Definir caminho completo do arquivo PDF
    arquivo_saida_pdf = os.path.join(pasta_rotas, f"{nome_arquivo}.pdf")
    
    pdf.cell(0, 10, titulo, ln=True, align="C")
    pdf.ln(10)

    # Informações gerais
    pdf.set_font("Arial", "B", 12)
    pdf.cell(0, 10, "Informações Gerais:", ln=True)
    pdf.set_font("Arial", "", 12)
    pdf.cell(0, 10, f"Ponto de Partida: {ponto_partida}", ln=True)
    pdf.cell(0, 10, f"Distância Total Estimada: {distancia_total:.2f} km", ln=True)
    pdf.cell(0, 10, f"Número de Entregas: {len(enderecos_ordenados) - 1}", ln=True)
    pdf.cell(0, 10, f"Total de Endereços com Erro: {len(enderecos_com_erro)}", ln=True)
    pdf.ln(10)

    # Cabeçalho da tabela
    pdf.set_font("Arial", "B", 12)
    pdf.set_fill_color(255, 0, 0)  # Vermelho
    pdf.set_text_color(255, 255, 255)  # Texto branco
    pdf.cell(10, 10, "Nº", 1, 0, "C", True)
    pdf.cell(50, 10, "Nome", 1, 0, "C", True)
    pdf.cell(70, 10, "Endereço", 1, 0, "C", True)
    pdf.cell(30, 10, "Distância", 1, 0, "C", True)
    pdf.cell(30, 10, "Link", 1, 1, "C", True)

    # Dados da tabela
    pdf.set_font("Arial", "", 10)
    pdf.set_text_color(0, 0, 0)  # Texto preto
    pdf.set_fill_color(255, 240, 240)
    for i, (nome, endereco, link, dist) in enumerate(zip(nomes_ordenados[1:], enderecos_ordenados[1:], links[1:], distancias_parciais), 1):
        # Verifica se precisa de nova página
        if pdf.get_y() > 250:
            pdf.add_page()
            # Recria o cabeçalho na nova página
            pdf.set_font("Arial", "B", 12)
            pdf.set_fill_color(255, 0, 0)  # Vermelho
            pdf.set_text_color(255, 255, 255)  # Texto branco
            pdf.cell(10, 10, "Nº", 1, 0, "C", True)
            pdf.cell(50, 10, "Nome", 1, 0, "C", True)
            pdf.cell(70, 10, "Endereço", 1, 0, "C", True)
            pdf.cell(30, 10, "Distância", 1, 0, "C", True)
            pdf.cell(30, 10, "Link", 1, 1, "C", True)
            pdf.set_font("Arial", "", 10)
            pdf.set_text_color(0, 0, 0)  # Texto preto
            pdf.set_fill_color(255, 240, 240)  # Fundo vermelho claro

        # Número
        pdf.cell(10, 15, '#' + str(i), 1, 0, "C", True)
        
        # Nome
        pdf.cell(50, 15, nome, 1, 0, "L", True)
        
        # Endereço
        pdf.cell(70, 15, endereco, 1, 0, "L", True)
        
        # Distância (arredondada para 1 casa decimal)
        pdf.cell(30, 15, f"{round(dist, 1)} km", 1, 0, "C", True)
        
        # Link (clicável)
        pdf.set_text_color(255, 0, 0)  # Texto vermelho
        pdf.cell(30, 15, "Ver no Maps", 1, 1, "C", True, link=link)
        pdf.set_text_color(0, 0, 0)  # Volta para texto preto

    # Adicionar seção de endereços com erro
    if enderecos_com_erro:
        pdf.add_page()
        pdf.set_font("Arial", "B", 14)
        pdf.cell(0, 10, "Endereços com Erro na Geocodificação", ln=True, align="C")
        pdf.ln(10)

        # Cabeçalho da tabela de erros
        pdf.set_font("Arial", "B", 12)
        pdf.set_fill_color(255, 0, 0)  # Vermelho
        pdf.set_text_color(255, 255, 255)  # Texto branco
        pdf.cell(20, 10, "Linha", 1, 0, "C", True)
        pdf.cell(50, 10, "Nome", 1, 0, "C", True)
        pdf.cell(120, 10, "Endereço", 1, 1, "C", True)

        # Dados dos erros
        pdf.set_font("Arial", "", 10)
        pdf.set_text_color(0, 0, 0)  # Texto preto
        pdf.set_fill_color(255, 240, 240)  # Fundo vermelho claro
        for linha, endereco in enderecos_com_erro:
            # Verifica se precisa de nova página
            if pdf.get_y() > 250:
                pdf.add_page()
                # Recria o cabeçalho na nova página
                pdf.set_font("Arial", "B", 12)
                pdf.set_fill_color(255, 0, 0)  # Vermelho
                pdf.set_text_color(255, 255, 255)  # Texto branco
                pdf.cell(20, 10, "Linha", 1, 0, "C", True)
                pdf.cell(50, 10, "Nome", 1, 0, "C", True)
                pdf.cell(120, 10, "Endereço", 1, 1, "C", True)
                pdf.set_font("Arial", "", 10)
                pdf.set_text_color(0, 0, 0)  # Texto preto
                pdf.set_fill_color(255, 240, 240)  # Fundo vermelho claro

            # Obtém o nome do cliente para este endereço
            nome_cliente = nomes[linha - 1] if linha - 1 < len(nomes) else ""

            pdf.cell(20, 15, str(linha), 1, 0, "C", True)
            pdf.cell(50, 15, nome_cliente, 1, 0, "L", True)
            pdf.cell(120, 15, endereco, 1, 1, "L", True)

    pdf.output(arquivo_saida_pdf)
    print_colorido(f"\n✅ PDF gerado com sucesso: {arquivo_saida_pdf}", Fore.GREEN)

except Exception as e:
    print_colorido(f"\n❌ Erro inesperado: {str(e)}", Fore.RED)
    import traceback
    print_colorido("Detalhes do erro:", Fore.RED)
    print_colorido(traceback.format_exc(), Fore.RED)
    exit(1)
