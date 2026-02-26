import psycopg2
import threading
import time
import uuid

# Configurações de Conexão
DB_CONFIG = {
    "host": "pgpool",
    "port": 9999,
    "database": "meu_banco",
    "user": "usuario_padrao",
    "password": "senha_padrao"
}

def get_conn():
    return psycopg2.connect(**DB_CONFIG)

def preparar_ambiente_complexo(num_registros=10000):
    print(f"--- Fase 1: Inserindo {num_registros} registros ---")
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS teste_sobrecarga;")
        cur.execute("""
            CREATE TABLE teste_sobrecarga (
                id SERIAL PRIMARY KEY,
                dado_unico TEXT UNIQUE,
                valor_numerico DOUBLE PRECISION,
                texto_longo TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        # Inserção em massa para ser mais rápido
        dados = [(str(uuid.uuid4()), i * 1.5, "carga_pesada_distribuida " * 15) for i in range(num_registros)]
        args_str = ','.join(cur.mogrify("(%s,%s,%s)", x).decode('utf-8') for x in dados)
        cur.execute("INSERT INTO teste_sobrecarga (dado_unico, valor_numerico, texto_longo) VALUES " + args_str)
        conn.commit()
        cur.close()
        conn.close()
        print("Ambiente pronto!\n")
    except Exception as e:
        print(f"Erro no Setup: {e}")

def realizar_query_complexa(contador):
    try:
        conn = get_conn()
        cur = conn.cursor()
        # Query pesada para forçar o uso de CPU e justificar o Load Balance
        query = """
            SELECT 
                AVG(SQRT(valor_numerico) * SIN(valor_numerico)), 
                COUNT(*) 
            FROM teste_sobrecarga 
            WHERE texto_longo ILIKE '%pesada%' 
            GROUP BY (id % 10);
        """
        cur.execute(query)
        cur.fetchall()
        cur.close()
        conn.close()
        contador[0] += 1
        return True
    except:
        return False

def executar_teste_carga(num_threads=20, duracao=20):
    print(f"--- Fase 2: Teste de Carga ({num_threads} threads, {duracao}s) ---")
    total_queries = [0]
    
    def worker():
        fim = time.time() + duracao
        while time.time() < fim:
            # CORREÇÃO AQUI: Chamando o nome correto da função
            realizar_query_complexa(total_queries)

    threads = [threading.Thread(target=worker) for _ in range(num_threads)]
    
    tempo_inicio = time.time()
    for t in threads: t.start()
    for t in threads: t.join()
    tempo_fim = time.time()
    
    total_tempo = tempo_fim - tempo_inicio
    qps = total_queries[0] / total_tempo
    
    print(f"Total de consultas: {total_queries[0]}")
    print(f"Tempo total: {total_tempo:.2f}s")
    print(f"Performance: {qps:.2f} QPS")
    return qps

def aguardar_replicacao():
    print("--- Verificando Sincronização das Réplicas ---")
    tentativas = 0
    while tentativas < 10:
        try:
            # Conecta direto na réplica para checar
            conn = psycopg2.connect(
                host="pg_replica_1", port=5432, 
                database="meu_banco", user="usuario_padrao", password="senha_padrao"
            )
            cur = conn.cursor()
            cur.execute("SELECT count(*) FROM teste_sobrecarga;")
            count = cur.fetchone()[0]
            cur.close()
            conn.close()
            
            if count >= 10000: # Se a réplica já tem os dados
                print(f"Réplicas sincronizadas! ({count} registros encontrados)")
                return True
        except Exception:
            pass
        
        print("Aguardando réplicas... (5s)")
        time.sleep(5)
        tentativas += 1
    return False


if __name__ == "__main__":
    # 1. Cria os dados no mestre
    preparar_ambiente_complexo(10000) 
    executar_teste_carga(num_threads=60, duracao=20)
    
    # 2. ESPERA as réplicas copiarem os dados
    # if aguardar_replicacao():
    #     # 3. Agora sim, roda o teste de carga
    #     executar_teste_carga(num_threads=60, duracao=20)
    # else:
    #     print("Erro: As réplicas não sincronizaram a tempo.")