#!/bin/bash

# Pegando os argumentos de forma explícita conforme a documentação do PgPool
FALLEN_NODE_ID=$1
FALLEN_NODE_HOST=$2
NEW_MASTER_NODE_ID=$5 # %m
NEW_MASTER_HOST=$8    # %H

echo "--- INICIANDO FAILOVER ---"
echo "Nó caído: ID $FALLEN_NODE_ID ($FALLEN_NODE_HOST)"
echo "Sugestão de Novo Mestre: ID $NEW_MASTER_NODE_ID ($NEW_MASTER_HOST)"

# Se o novo mestre sugerido for vazio ou for o próprio nó que caiu, 
# tentamos determinar o próximo nó disponível (lógica simples de fallback)
if [ -z "$NEW_MASTER_HOST" ] || [ "$NEW_MASTER_HOST" == "$FALLEN_NODE_HOST" ]; then
    echo "Aviso: Host de destino inválido. Tentando determinar novo mestre manualmente..."
    if [ "$FALLEN_NODE_ID" == "0" ]; then 
        NEW_MASTER_HOST="pg_replica_1"; NEW_MASTER_NODE_ID=1;
    elif [ "$FALLEN_NODE_ID" == "1" ]; then 
        NEW_MASTER_HOST="pg_replica_2"; NEW_MASTER_NODE_ID=2;
    fi
fi

echo "Executando promoção SQL em: $NEW_MASTER_HOST"

# 1. Comando de Promoção (Executado de forma isolada para evitar erros de transação)
psql -h $NEW_MASTER_HOST -p 5432 -U usuario_padrao -d meu_banco -c "SELECT pg_promote();"

# 2. Reconfigurar as outras réplicas (Follow Primary)
NODES=("pg_primary" "pg_replica_1" "pg_replica_2")

for i in "${!NODES[@]}"; do
    NODE_HOST=${NODES[$i]}
    
    # Se o nó não for o que caiu e não for o novo mestre, ele deve seguir o novo mestre
    if [ $i -ne $FALLEN_NODE_ID ] && [ $i -ne $NEW_MASTER_NODE_ID ]; then
        echo "Avisando réplica $NODE_HOST para seguir $NEW_MASTER_HOST..."
        
        # Executando comandos separadamente para evitar "ALTER SYSTEM cannot run inside a transaction block"
        psql -h $NODE_HOST -p 5432 -U usuario_padrao -d meu_banco -c "ALTER SYSTEM SET primary_conninfo = 'host=$NEW_MASTER_HOST port=5432 user=usuario_padrao password=senha_padrao';"
        psql -h $NODE_HOST -p 5432 -U usuario_padrao -d meu_banco -c "ALTER SYSTEM SET recovery_target_timeline = 'latest';"
        psql -h $NODE_HOST -p 5432 -U usuario_padrao -d meu_banco -c "SELECT pg_reload_conf();"
        
        # Mata o processo receptor de WAL antigo para forçar a reconexão imediata ao novo mestre
        psql -h $NODE_HOST -p 5432 -U usuario_padrao -d meu_banco -c "SELECT pg_terminate_backend(pid) FROM pg_stat_wal_receiver;"
    fi
done

echo "--- FAILOVER FINALIZADO ---"
exit 0