import pika
import json
import mysql.connector
from mysql.connector import Error
import logging
from typing import Tuple, Dict, Any, Optional
import os
from datetime import datetime

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DatabaseConsumer:
    def __init__(self, host: str = 'localhost'):
        """
        Inicializa o consumidor do banco de dados
        :param host: Host do RabbitMQ
        """
        # Configurações do banco de dados
        self.db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'user': os.getenv('DB_USER', 'root'),
            'password': os.getenv('DB_PASSWORD', '1234'),
            'database': os.getenv('DB_DATABASE', 'banco_sistema')
        }
        
        self.rabbitmq_host = host
        self.setup_rabbitmq_connection()

    def setup_rabbitmq_connection(self) -> None:
        """Estabelece conexão com o RabbitMQ e configura as filas"""
        try:
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=self.rabbitmq_host)
            )
            self.channel = self.connection.channel()
            
            # Declaração das filas com persistência
            self.channel.queue_declare(queue='Fila_2', durable=True)
            self.channel.queue_declare(queue='Fila_3', durable=True)
            
            # Configuração de QoS
            self.channel.basic_qos(prefetch_count=1)
            
        except Exception as e:
            logger.error(f"Erro ao conectar ao RabbitMQ: {str(e)}")
            raise

    def get_db_connection(self) -> Optional[mysql.connector.MySQLConnection]:
        """
        Cria uma nova conexão com o banco de dados
        :return: Conexão com o banco de dados ou None em caso de erro
        """
        try:
            return mysql.connector.connect(**self.db_config)
        except Error as e:
            logger.error(f"Erro ao conectar ao banco de dados: {str(e)}")
            return None

    def save_to_database(self, user_data: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Salva os dados do usuário no banco de dados
        :param user_data: Dados do usuário a serem salvos
        :return: Tupla com status da operação e mensagem
        """
        conn = self.get_db_connection()
        if not conn:
            return False, "Erro ao conectar ao banco de dados"

        try:
            cursor = conn.cursor()
            
            # Inserir usuário
            insert_query = '''
            INSERT INTO usuarios (nome, cpf, email, telefone, conta, tipo, saldo)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            '''
            values = (
                user_data['nome'],
                user_data['cpf'],
                user_data['email'],
                user_data['telefone'],
                user_data['conta'],
                user_data['tipo'],
                user_data['saldo']
            )
            
            cursor.execute(insert_query, values)
            usuario_id = cursor.lastrowid
            
            # Registrar transação inicial
            if float(user_data['saldo']) > 0:
               trans_query = '''
                INSERT INTO transacoes (usuario_id, tipo, valor, data_transacao)
                VALUES (%s, %s, %s, NOW())
                '''
            cursor.execute(trans_query, (usuario_id, 'deposito_inicial', float(user_data['saldo'])))            
            conn.commit()
            return True, f"Usuário cadastrado com sucesso. ID: {usuario_id}"
            
        except Error as e:
            if conn:
                conn.rollback()
            error_msg = str(e)
            if "Duplicate entry" in error_msg:
                if "cpf" in error_msg:
                    return False, "CPF já cadastrado"
                elif "conta" in error_msg:
                    return False, "Número de conta já existe"
                elif "email" in error_msg:
                    return False, "E-mail já cadastrado"
            return False, f"Erro ao salvar no banco: {error_msg}"
            
        finally:
            if conn and conn.is_connected():
                cursor.close()
                conn.close()

    def process_message(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Processa a mensagem recebida
        :param data: Dados recebidos da mensagem
        :return: Resultado do processamento
        """
        if data.get('status') != 'success':
            return {
                "status": "error",
                "message": "Dados inválidos recebidos",
                "data": data
            }

        user_data = data.get('data', {})
        success, message = self.save_to_database(user_data)
        
        return {
            "status": "success" if success else "error",
            "message": message,
            "data": user_data if success else None,
            "timestamp": datetime.now().isoformat()
        }

    def callback(self, ch, method, properties, body: bytes) -> None:
        """
        Callback para processar mensagens recebidas
        :param ch: Canal do RabbitMQ
        :param method: Método de entrega
        :param properties: Propriedades da mensagem
        :param body: Corpo da mensagem
        """
        try:
            # Decodifica a mensagem
            data = json.loads(body)
            logger.info(f"Mensagem recebida para processamento: {data}")
            
            # Processa a mensagem
            result = self.process_message(data)
            
            # Publica o resultado
            self.channel.basic_publish(
                exchange='',
                routing_key='Fila_3',
                body=json.dumps(result),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Mensagem persistente
                    content_type='application/json'
                )
            )
            
            logger.info(f"Processamento concluído: {result['status']}")
            
        except json.JSONDecodeError:
            logger.error("Erro: Mensagem não está no formato JSON válido")
            self.publish_error("Formato JSON inválido", body.decode())
        except Exception as e:
            logger.error(f"Erro no processamento: {str(e)}")
            self.publish_error(str(e))
        finally:
            ch.basic_ack(delivery_tag=method.delivery_tag)

    def publish_error(self, error_message: str, data: Any = None) -> None:
        """
        Publica mensagem de erro na Fila_3
        :param error_message: Mensagem de erro
        :param data: Dados relacionados ao erro
        """
        error_response = {
            "status": "error",
            "message": error_message,
            "data": data,
            "timestamp": datetime.now().isoformat()
        }
        
        self.channel.basic_publish(
            exchange='',
            routing_key='Fila_3',
            body=json.dumps(error_response),
            properties=pika.BasicProperties(delivery_mode=2)
        )

    def start(self) -> None:
        """Inicia o consumo de mensagens"""
        try:
            self.channel.basic_consume(
                queue='Fila_2',
                on_message_callback=self.callback
            )
            
            logger.info('Consumidor do banco de dados iniciado. Aguardando mensagens...')
            self.channel.start_consuming()
            
        except KeyboardInterrupt:
            logger.info("Consumidor interrompido pelo usuário")
            self.stop()
        except Exception as e:
            logger.error(f"Erro durante o consumo de mensagens: {str(e)}")
            self.stop()

    def stop(self) -> None:
        """Para o consumidor e fecha conexões"""
        try:
            if self.channel and not self.channel.is_closed:
                self.channel.close()
            if self.connection and not self.connection.is_closed:
                self.connection.close()
            logger.info("Conexões fechadas")
        except Exception as e:
            logger.error(f"Erro ao fechar conexões: {str(e)}")

if __name__ == "__main__":
    try:
        consumer = DatabaseConsumer()
        consumer.start()
    except Exception as e:
        logger.error(f"Erro ao iniciar o consumidor: {str(e)}")