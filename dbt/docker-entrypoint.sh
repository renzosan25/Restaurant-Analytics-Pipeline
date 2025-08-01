#!/bin/sh
set -e #Termina si pasa algo (error)

echo "Esperando a que PostgresSQL este disponible en $DB_HOST:$DB_PORT...."
/wait-for-it.sh "$DB_HOST:$DB_PORT" --timeout=30 --strict
echo "POSTGRESQL disponible. Ejecutando comando...."

exec "$@"

