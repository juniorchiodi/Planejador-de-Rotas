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
        'User-Agent': 'RotaEntregas/1.0 (https://github.com/seu-usuario/rota-entregas; seu-email@exemplo.com) Python/3.x'
    }

    for tentativa in range(max_tentativas):
        try:
            print_colorido(f"\n🔍 Geocodificando: {endereco}", Fore.CYAN)
            print_colorido(f"   Tentativa {tentativa + 1}/{max_tentativas}", Fore.YELLOW)
            
            response = requests.get(url, headers=headers, timeout=15)
            
            if response.status_code == 403:
                print_colorido("⚠️ Erro 403: Acesso negado pela API. Aguardando 30 segundos...", Fore.RED)
                time.sleep(30)
                continue
                
            response.raise_for_status()
            
            data = response.json()
            if data:
                lat = float(data[0]['lat'])
                lon = float(data[0]['lon'])
                print_colorido(f"✅ Coordenadas encontradas: {lat}, {lon}", Fore.GREEN)
                time.sleep(2)
                return {
                    'coords': (lat, lon),
                    'timestamp': datetime.now().isoformat()
                }
            else:
                print_colorido(f"❌ Não foi possível encontrar coordenadas para: {endereco}", Fore.RED)
                
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            print_colorido(f"⚠️ Erro de conexão na tentativa {tentativa + 1}: {str(e)}", Fore.RED)
            if tentativa < max_tentativas - 1:
                tempo_espera = (tentativa + 1) * 10
                print_colorido(f"   Aguardando {tempo_espera} segundos...", Fore.YELLOW)
                time.sleep(tempo_espera)
            continue
        except Exception as e:
            print_colorido(f"❌ Erro inesperado: {str(e)}", Fore.RED)
            if tentativa < max_tentativas - 1:
                print_colorido("   Aguardando 15 segundos...", Fore.YELLOW)
                time.sleep(15)
            continue
    
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
        
        # Validação da distância - aumentado para 500km para permitir rotas entre cidades
        if dist > 500:  # Se a distância for maior que 500km, provavelmente está errado
            print(f"Distância suspeita: {dist:.2f}km entre ({lat1}, {lon1}) e ({lat2}, {lon2})")
            return float('inf')
        
        # Arredonda para 2 casas decimais
        return round(dist, 2)
    except Exception as e:
        print(f"Erro ao calcular distância: {str(e)}")
        return float('inf')

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
    pontos_nao_visitados = set(range(1, n))  # Todos os pontos exceto o de partida
    
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
            print(f"Usando cache para: {endereco}")
            return endereco, cache[endereco]['coords']
        
        resultado = geocodificar_endereco(endereco)
        if resultado:
            cache[endereco] = resultado
            salvar_cache(cache)
            return endereco, resultado['coords']
        return None

    # Processar endereços em paralelo
    print_colorido("\n🔄 Geocodificando endereços...", Fore.CYAN)
    with ThreadPoolExecutor(max_workers=1) as executor:
        resultados = list(tqdm(executor.map(processar_endereco, enderecos), 
                             total=len(enderecos),
                             desc="Progresso",
                             unit="endereço"))
    
    # Filtrar resultados válidos e coletar erros
    for i, (endereco, resultado) in enumerate(zip(enderecos, resultados), 1):
        if resultado:
            endereco, coords = resultado
            coordenadas.append(coords)
            enderecos_validos.append(endereco)
        else:
            enderecos_com_erro.append((i, endereco))

    if len(enderecos_validos) <= 1:
        print_colorido("❌ Erro: Nenhum endereço foi geocodificado com sucesso além do ponto de partida.", Fore.RED)
        exit(1)

    print_colorido(f"\n✅ Total de endereços geocodificados com sucesso: {len(enderecos_validos)}", Fore.GREEN)
    print_colorido(f"⚠️ Total de endereços com erro: {len(enderecos_com_erro)}", Fore.YELLOW)

    # MATRIZ DE DISTÂNCIA
    print_colorido("\n📏 Calculando matriz de distância...", Fore.CYAN)
    n = len(coordenadas)
    dist_matrix = np.zeros((n, n))

    for i in tqdm(range(n), desc="Calculando distâncias", unit="ponto"):
        for j in range(n):
            if i != j:
                dist = calcular_distancia_rua(coordenadas[i], coordenadas[j])
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
    data_atual = datetime.now().strftime("%d/%m/%Y")
    
    # Criar título com cidade e data
    titulo = f"Rota de Entregas - {cidade} - {data_atual}"
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
    pdf.cell(10, 10, "Nº", 1, 0, "C")
    pdf.cell(50, 10, "Nome", 1, 0, "C")
    pdf.cell(70, 10, "Endereço", 1, 0, "C")
    pdf.cell(30, 10, "Distância", 1, 0, "C")
    pdf.cell(30, 10, "Link", 1, 1, "C")

    # Dados da tabela
    pdf.set_font("Arial", "", 10)
    for i, (nome, endereco, link, dist) in enumerate(zip(nomes_ordenados[1:], enderecos_ordenados[1:], links[1:], distancias_parciais), 1):
        # Verifica se precisa de nova página
        if pdf.get_y() > 250:
            pdf.add_page()
            # Recria o cabeçalho na nova página
            pdf.set_font("Arial", "B", 12)
            pdf.cell(10, 10, "Nº", 1, 0, "C")
            pdf.cell(50, 10, "Nome", 1, 0, "C")
            pdf.cell(70, 10, "Endereço", 1, 0, "C")
            pdf.cell(30, 10, "Distância", 1, 0, "C")
            pdf.cell(30, 10, "Link", 1, 1, "C")
            pdf.set_font("Arial", "", 10)

        # Número
        pdf.cell(10, 15, '#' + str(i), 1, 0, "C")
        
        # Nome
        pdf.cell(50, 15, nome, 1, 0, "L")
        
        # Endereço
        pdf.cell(70, 15, endereco, 1, 0, "L")
        
        # Distância
        pdf.cell(30, 15, f"{dist:.2f} km", 1, 0, "C")
        
        # Link (clicável)
        pdf.cell(30, 15, "Ver no Maps", 1, 1, "C", link=link)

    # Adicionar seção de endereços com erro
    if enderecos_com_erro:
        pdf.add_page()
        pdf.set_font("Arial", "B", 14)
        pdf.cell(0, 10, "Endereços com Erro na Geocodificação", ln=True, align="C")
        pdf.ln(10)

        # Cabeçalho da tabela de erros
        pdf.set_font("Arial", "B", 12)
        pdf.cell(20, 10, "Linha", 1, 0, "C")
        pdf.cell(170, 10, "Endereço", 1, 1, "C")

        # Dados dos erros
        pdf.set_font("Arial", "", 10)
        for linha, endereco in enderecos_com_erro:
            # Verifica se precisa de nova página
            if pdf.get_y() > 250:
                pdf.add_page()
                # Recria o cabeçalho na nova página
                pdf.set_font("Arial", "B", 12)
                pdf.cell(20, 10, "Linha", 1, 0, "C")
                pdf.cell(170, 10, "Endereço", 1, 1, "C")
                pdf.set_font("Arial", "", 10)

            pdf.cell(20, 15, str(linha), 1, 0, "C")
            pdf.cell(170, 15, endereco, 1, 1, "L")

    pdf.output(arquivo_saida_pdf)
    print_colorido(f"\n✅ PDF gerado com sucesso: {arquivo_saida_pdf}", Fore.GREEN)

except Exception as e:
    print_colorido(f"\n❌ Erro inesperado: {str(e)}", Fore.RED)
    import traceback
    print_colorido("Detalhes do erro:", Fore.RED)
    print_colorido(traceback.format_exc(), Fore.RED)
    exit(1)
