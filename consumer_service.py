import pika
import json
import re
import logging
from typing import Tuple, List, Dict, Any, Union

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BusinessRuleConsumer:
    def __init__(self, host: str = 'localhost'):
        self.host = host
        self.connection = None
        self.channel = None
        self.setup_rabbitmq_connection()

    def setup_rabbitmq_connection(self) -> None:
        try:
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=self.host)
            )
            self.channel = self.connection.channel()
            
            self.channel.queue_declare(queue='Fila_1', durable=True)
            self.channel.queue_declare(queue='Fila_2', durable=True)
            self.channel.queue_declare(queue='Fila_3', durable=True)
            
            self.channel.basic_qos(prefetch_count=1)
            
        except Exception as e:
            logger.error(f"Erro ao conectar ao RabbitMQ: {str(e)}")
            raise

    def validate_cpf(self, cpf: str) -> Tuple[bool, str]:
        """
        Valida o formato e dígitos verificadores do CPF
        :param cpf: string contendo o CPF
        :return: tupla com resultado da validação e mensagem de erro
        """
        # Remove caracteres não numéricos
        cpf = re.sub(r'[^0-9]', '', cpf)
        
        # Verifica formato básico
        if not re.match(r'^\d{11}$', cpf):
            return False, "CPF deve conter exatamente 11 dígitos numéricos"
            
        # Verifica se todos os dígitos são iguais
        if len(set(cpf)) == 1:
            return False, "CPF inválido"
            
        # Validação dos dígitos verificadores
        for i in range(9, 11):
            value = sum((int(cpf[num]) * ((i + 1) - num) for num in range(0, i)))
            digit = ((value * 10) % 11) % 10
            if digit != int(cpf[i]):
                return False, "CPF inválido"
                
        return True, ""

    def validate_email(self, email: str) -> Tuple[bool, str]:
        """
        Valida o formato do email
        :param email: string contendo o email
        :return: tupla com resultado da validação e mensagem de erro
        """
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if not re.match(pattern, email):
            return False, "Formato de email inválido"
        return True, ""

    def validate_account(self, account: str) -> Tuple[bool, str]:
        """
        Valida o número da conta
        :param account: string contendo o número da conta
        :return: tupla com resultado da validação e mensagem de erro
        """
        # Remove caracteres não numéricos
        account = re.sub(r'[^0-9]', '', account)
        
        if not account or len(account) < 5:
            return False, "Número da conta deve ter no mínimo 5 dígitos"
        return True, ""

    def validate_phone(self, phone: str) -> Tuple[bool, str]:
        """
        Valida o número de telefone
        :param phone: string contendo o telefone
        :return: tupla com resultado da validação e mensagem de erro
        """
        # Remove caracteres não numéricos
        phone = re.sub(r'[^0-9]', '', phone)
        
        if not re.match(r'^\d{10,11}$', phone):
            return False, "Telefone deve ter 10 ou 11 dígitos numéricos"
        return True, ""

    def validate_user_data(self, user_data: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Realiza todas as validações nos dados do usuário
        :param user_data: dicionário com os dados do usuário
        :return: tupla com resultado da validação e lista de erros
        """
        errors = []
        
        # Validações de campos obrigatórios
        required_fields = ['nome', 'cpf', 'email', 'telefone', 'conta', 'tipo', 'saldo']        
        for field in required_fields:
            if field not in user_data or not user_data[field]:
                errors.append(f"Campo {field} é obrigatório")
    
        if errors:
            return False, errors  # Retorna imediatamente em caso de erro
    
        # Validação do nome
        if len(user_data['nome'].strip()) < 3:
            errors.append("Nome deve ter pelo menos 3 caracteres")
        
        # Validações específicas
        validations = [
            self.validate_cpf(user_data['cpf']),
            self.validate_email(user_data['email']),
            self.validate_account(user_data['conta']),
            self.validate_phone(user_data['telefone']),
        ]
    
        for valid, error_msg in validations:
            if not valid:
                errors.append(error_msg)
    
        return len(errors) == 0, errors

    def process_message(self, ch, method, properties, body):
        """
        Processa uma mensagem recebida
        """
        try:
            user_data = json.loads(body)
            is_valid, errors = self.validate_user_data(user_data)
            
            if is_valid:
                logger.info(f"Dados validados com sucesso: {user_data}")
                return {
                    'status': 'success',
                    'data': user_data,
                    'errors': None
                }
            else:
                logger.warning(f"Falha na validação dos dados: {errors}")
                return {
                    'status': 'error',
                    'data': user_data,
                    'errors': errors
                }
                
        except Exception as e:
            logger.error(f"Erro ao processar a mensagem: {e}")
            return {
                'status': 'error',
                'data': None,
                'message': str(e)
            }
        finally:
            ch.basic_ack(delivery_tag=method.delivery_tag)

    def callback(self, ch, method, properties, body: bytes) -> None:
        try:
            user_data = json.loads(body)
            logger.info(f"Mensagem recebida: {user_data}")
            
            # Chama process_message com todos os parâmetros necessários
            result = self.process_message(ch, method, properties, body)
            
            if result['status'] == 'success':
                self.channel.basic_publish(
                    exchange='',
                    routing_key='Fila_2',
                    body=json.dumps(result),
                    properties=pika.BasicProperties(
                        delivery_mode=2,
                        content_type='application/json'
                    )
                )
                logger.info("Mensagem processada e enviada para Fila_2")
            else:
                self.channel.basic_publish(
                    exchange='',
                    routing_key='Fila_1',
                    body=json.dumps({
                        "status": "error",
                        "message": "Falha na validação dos dados",
                        "errors": result.get('errors', []),
                        "original_data": result.get('data', {})
                    }),
                    properties=pika.BasicProperties(
                        delivery_mode=2,
                        content_type='application/json'
                    )
                )
                logger.info("Mensagem com erro reenviada para Fila_1")
                
        except json.JSONDecodeError as e:
            logger.error("Erro: Mensagem não está no formato JSON válido")
            self.channel.basic_publish(
                exchange='',
                routing_key='Fila_1',
                body=json.dumps({
                    "status": "error",
                    "message": "Formato JSON inválido",
                    "data": body.decode()
                }),
                properties=pika.BasicProperties(delivery_mode=2)
            )
            
        except Exception as e:
            logger.error(f"Erro no processamento: {str(e)}")
            self.channel.basic_publish(
                exchange='',
                routing_key='Fila_1',
                body=json.dumps({
                    "status": "error",
                    "message": f"Erro no processamento: {str(e)}",
                    "data": None
                }),
                properties=pika.BasicProperties(delivery_mode=2)
            )

    def start(self) -> None:
        try:
            self.channel.basic_consume(
                queue='Fila_1',
                on_message_callback=self.callback
            )
            logger.info('Consumidor iniciado. Aguardando mensagens...')
            self.channel.start_consuming()
            
        except KeyboardInterrupt:
            self.stop()
        except Exception as e:
            logger.error(f"Erro durante o consumo: {str(e)}")
            self.stop()

    def stop(self) -> None:
        try:
            if self.channel and self.channel.is_open:
                self.channel.close()
            if self.connection and self.connection.is_open:
                self.connection.close()
            logger.info("Conexões fechadas")
        except Exception as e:
            logger.error(f"Erro ao fechar conexões: {str(e)}")

if __name__ == "__main__":
    consumer = BusinessRuleConsumer()
    try:
        consumer.start()
    except Exception as e:
        logger.error(f"Erro fatal: {str(e)}")