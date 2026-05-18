# Enigma Medallion Data Pipeline

Este projeto implementa um pipeline de engenharia de dados baseado na arquitetura Medallion (Bronze, Silver e Gold), estruturado de forma desacoplada e modular em Python para simular cenários reais de DataOps e processamento em streaming.

O pipeline é projetado para lidar com dados de streaming que chegam fora de ordem e com mensagens duplicadas de rede, realizando ordenação temporal com o campo `part_index` e deduplicação cronológica idempotente.

## 📐 Arquitetura do Pipeline

![Arquitetura do Pipeline](docs/arquitetura.jpeg)


## 📂 Estrutura do Projeto

*   `consumer/`: Ingestão do stream de dados e validação de schemas básicos.
*   `bronze/`: Landing zone crua que grava os arquivos originais com metadados.
*   `silver/`: Processamento, higienização, deduplicação e ordenação de índices.
*   `gold/`: Reconstituição inteligente da mensagem final e auditoria de qualidade.
*   `api/`: API REST e Dashboard de monitoramento em tempo real com FastAPI.
*   `tests/`: Suíte de testes automatizados com Pytest.

## 🚀 Como Executar

### 1. Ativar o Ambiente Virtual
```powershell
.\.venv\Scripts\Activate.ps1
```

### 2. Executar a Simulação do Pipeline
```powershell
python run_pipeline.py
```

### 3. Executar os Testes Unitários
```powershell
pytest -v
```

### 4. Rodar o Servidor de API e o Dashboard
```powershell
python -m uvicorn api.app:app --reload
```
Acesse o painel interativo em: `http://127.0.0.1:8000`
