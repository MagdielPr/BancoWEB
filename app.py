from flask import Flask, request, render_template, jsonify, redirect, url_for, session
import pika
import json
from flask_cors import CORS
from functools import wraps
import mysql.connector
from werkzeug.security import check_password_hash

app = Flask(__name__)
CORS(app)
app.secret_key = 'sua_chave_secreta_aqui'  # Adicione uma chave secreta para sessão

# Configurações do RabbitMQ
RABBITMQ_HOST = 'localhost'
RABBITMQ_QUEUE = 'Fila_1'

# Configurações do banco de dados
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '1234',
    'database': 'banco_sistema'
}

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):  
        if 'logged_in' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

def setup_rabbitmq_connection():
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
        channel = connection.channel()
        channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
        return connection, channel
    except Exception as e:
        print(f"Erro ao conectar com RabbitMQ: {str(e)}")
        return None, None

# Rota principal redireciona para login
@app.route('/')
def index():
    return redirect(url_for('login'))

# Rota de login
@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']

        try:
            conn = mysql.connector.connect(**DB_CONFIG)
            cursor = conn.cursor(dictionary=True)

            cursor.execute("SELECT * FROM usuarios_sistema WHERE username = %s", (username,))
            user = cursor.fetchone()

            if user and user['password'] == password:  # Em produção, use hash de senha
                session['logged_in'] = True
                session['username'] = username
                return redirect(url_for('menu'))
            else:
                return render_template('login.html', error="Credenciais inválidas")

        except Exception as e:
            return render_template('login.html', error="Erro ao fazer login")
        finally:
            if conn.is_connected():
                cursor.close()
                conn.close()

    return render_template('login.html')

# Rota do menu principal
@app.route('/menu')
@login_required
def menu():
    return render_template('menu.html')

# Rota para o formulário de cadastro
@app.route('/cadastro')
@login_required
def cadastro():
    return render_template('form.html')

# Rota para processar o formulário
@app.route('/send', methods=['POST'])
@login_required
def send_form():
    try:
        usuario = {
            "nome": request.form['nome'],
            "cpf": request.form['cpf'],
            "email": request.form['email'],
            "telefone": request.form['telefone'],
            "conta": request.form['conta'],
            "tipo": request.form['tipo'],
            "saldo": float(request.form['saldo'])
        }

        connection, channel = setup_rabbitmq_connection()
        if not connection or not channel:
            return jsonify({
                "status": "error",
                "message": "Erro ao conectar com o serviço de mensageria"
            }), 500

        try:
            channel.basic_publish(
                exchange='',
                routing_key=RABBITMQ_QUEUE,
                body=json.dumps(usuario),
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    content_type='application/json'
                )
            )

            return jsonify({
                "status": "success",
                "message": f"Cadastro do usuário {usuario['nome']} enviado para processamento"
            })

        except Exception as e:
            return jsonify({
                "status": "error",
                "message": f"Erro ao enviar mensagem: {str(e)}"
            }), 500

        finally:
            if connection and not connection.is_closed:
                connection.close()

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Erro ao processar requisição: {str(e)}"
        }), 400

# Rota para logout
@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))

@app.route('/consulta')
@login_required
def consulta():
    return render_template('consulta.html')

@app.route('/api/clientes')
@login_required
def buscar_clientes():
    search_term = request.args.get('search', '').strip()

    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)

        if search_term:
            # Busca com filtro
            query = """
                SELECT * FROM usuarios 
                WHERE nome LIKE %s 
                OR cpf LIKE %s 
                OR conta LIKE %s
                ORDER BY nome
            """
            search_pattern = f'%{search_term}%'
            cursor.execute(query, (search_pattern, search_pattern, search_pattern))
        else:
            # Busca todos os clientes
            cursor.execute("SELECT * FROM usuarios ORDER BY nome")

        clientes = cursor.fetchall()

        # Converter valores decimais para float para serialização JSON
        for cliente in clientes:
            if 'saldo' in cliente:
                cliente['saldo'] = float(cliente['saldo'])

        return jsonify(clientes)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)