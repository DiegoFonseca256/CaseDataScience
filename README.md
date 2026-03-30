📊 Hipótese Capital
O Hipótese Capital é uma ferramenta de inteligência de mercado focada em análise fundamentalista de ações da B3. O sistema automatiza a coleta de indicadores financeiros, notícias e gera análises profundas utilizando Inteligência Artificial (LLMs via Groq), permitindo que você tome decisões baseadas em dados consolidados.

🚀 Fluxo de Trabalho
O sistema utiliza um banco de dados SQLite para separar o que é dado estático (cadastro da empresa) do que é dinâmico (snapshots de mercado e análises da IA).

🛠️ Arquitetura
CharlesRiver/

├── .env                # Chaves de API (não versionar)

├── .gitignore          # Filtros do Git

├── dashboard.py        # Interface visual (Streamlit)

├── database.py         # Camada de persistência (SQLite)

├── LLM.py              # Integração com Groq Cloud

├── main.py             # Script principal de coleta (Pipeline)

├── pendentes.txt       # Fila de tickers para processamento

└── hipotese_capital.db # Banco de dados SQLite

⚙️ Instalação e Configuração
1. Pré-requisitos
Python 3.9+

Groq API Key (para as análises da IA)

NewsAPI Key ( para coleta de notícias)

2. Passos
Clone o repositório e acesse a pasta.

Instale as dependências:

# Bash

pip install -r requirements.txt

__________________________________

Configure o arquivo .env na raiz do projeto com suas chaves:


GROQ_API_KEY=sua_chave_aqui

NEWS_API_KEY=sua_chave_aqui

LLM_MODEL=llama-3.3-70b-versatile


Inicialize o banco de dados e rode a coleta:

# Bash
python main.py

streamlit run dashboard.py

__________________________________

Ao adicionar mais ticker no dashboard, deve-serepetir o mesmo processo para coletar os dados:

python main.py

streamlit run dashboard.py 
