# main.py
import sys
# Altere a linha de importação para:
from .business_rule_consumer import BusinessRuleConsumer  
from database_consumer import DatabaseConsumer
from result_consumer import ResultConsumer

def main():
    if len(sys.argv) != 2:   
        print("Uso: python main.py [service|database|result]")
        sys.exit(1)
        
    consumer_type = sys.argv[1].lower()
    
    try:
        if consumer_type == "service":
            consumer = BusinessRuleConsumer()
            print("Iniciando consumidor de regras de negócio...")
        elif consumer_type == "database":
            consumer = DatabaseConsumer()
            print("Iniciando consumidor de banco de dados...")
        elif consumer_type == "result":
            consumer = ResultConsumer()
            print("Iniciando consumidor de resultados...")
        else:
            print("Tipo de consumidor inválido. Use: service, database, ou result")
            sys.exit(1)
            
        consumer.start()
    except KeyboardInterrupt:
        print("\nEncerrando consumidor...")
    except Exception as e:
        print(f"Erro: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()