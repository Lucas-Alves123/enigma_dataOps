import os
import random
import json
import asyncio
from datetime import datetime
from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from typing import List, Optional

from consumer.receiver import ConsumerReceiver
from bronze.processor import BronzeProcessor
from silver.processor import SilverProcessor
from gold.processor import GoldProcessor
from ably import AblyRealtime

app = FastAPI(
    title="Medallion Pipeline API",
    version="1.0.0"
)

db_received_data: List[dict] = []

class GoldRecord(BaseModel):
    pipeline_run_id: str
    full_payload: str
    assembled_parts_count: int
    min_index: int
    max_index: int
    missing_indices: List[int]
    data_integrity_status: str
    processed_at: str

@app.post("/api/data", response_model=dict, status_code=201)
def receive_gold_data(record: GoldRecord):
    record_dict = record.model_dump()
    record_dict["received_at_api"] = datetime.now().isoformat()
    db_received_data.insert(0, record_dict)
    print(f"[API] Received Gold Run: {record.pipeline_run_id}")
    return {"status": "sucesso", "resposta": record.full_payload}

@app.get("/api/data", response_model=List[dict])
def get_gold_data():
    return db_received_data

class AblyState:
    client = None
    channel = None
    status = "Parado"
    accepted = 0
    ignored = 0
    invalid = 0
    last_error = ""
    last_gold = "sem resultado"
    channel_name = "enigma-stream"
    # Referência ao consumer atual para injeção de mock
    current_consumer = None
    
ably_state = AblyState()

class StartAblyRequest(BaseModel):
    api_key: str
    channel: str
    limpar_camadas: bool
    deduplicar_origem: bool

@app.post("/api/ably/start")
async def start_ably(payload: StartAblyRequest):
    if ably_state.status == "Rodando":
        return {"status": "Já rodando"}

    base_dir = "data"
    bronze_proc = BronzeProcessor(base_dir)
    
    if payload.limpar_camadas:
        bronze_proc.clear()
        SilverProcessor(base_dir).clear()
        GoldProcessor(base_dir).clear()

    consumer = ConsumerReceiver(bronze_proc)
    ably_state.current_consumer = consumer
    
    ably_state.channel_name = payload.channel
    ably_state.accepted = 0
    ably_state.ignored = 0
    ably_state.invalid = 0
    ably_state.last_error = ""
    ably_state.status = "Conectando..."

    try:
        ably_state.client = AblyRealtime(payload.api_key)
        await ably_state.client.connection.once_async('connected')
        ably_state.channel = ably_state.client.channels.get(payload.channel)
        
        def listener(message):
            try:
                data = message.data
                if isinstance(data, str):
                    data = json.loads(data)
                ably_state.current_consumer.receive_message(data)
                ably_state.accepted += 1
            except Exception as e:
                ably_state.invalid += 1
                ably_state.last_error = f"{type(e).__name__}: {str(e)}"

        await ably_state.channel.subscribe(listener)
        ably_state.status = "Rodando"
        return {"status": "Iniciado com sucesso"}
    except Exception as e:
        ably_state.status = "Parado"
        ably_state.last_error = f"Erro na conexão: {str(e)}"
        if ably_state.client:
            await ably_state.client.close()
            ably_state.client = None
        return {"status": "Erro"}

@app.post("/api/ably/mock_produce")
async def mock_produce(payload: dict):
    if ably_state.status != "Rodando" or not ably_state.current_consumer:
        return {"status": "Erro: Pipeline não está rodando. Clique em Iniciar no painel primeiro."}
    try:
        ably_state.current_consumer.receive_message(payload)
        ably_state.accepted += 1
        return {"status": "ok"}
    except Exception as e:
        ably_state.invalid += 1
        ably_state.last_error = f"Mock Error: {str(e)}"
        return {"status": "error"}

@app.post("/api/ably/stop")
async def stop_ably():
    if ably_state.status in ["Rodando", "Conectando..."]:
        if ably_state.client:
            try:
                await ably_state.channel.unsubscribe()
            except:
                pass
            await ably_state.client.close()
            ably_state.client = None
            ably_state.channel = None
        
        ably_state.status = "Processando Medallion..."
        
        try:
            base_dir = "data"
            bronze_proc = BronzeProcessor(base_dir)
            silver_proc = SilverProcessor(base_dir, bronze_proc)
            gold_proc = GoldProcessor(base_dir, silver_proc, api_url="http://127.0.0.1:8000/api/data")
            
            silver_proc.process_bronze_data()
            gold_record = gold_proc.process_silver_data()
            if gold_record:
                gold_proc.send_to_api(gold_record)
                ably_state.last_gold = gold_record.get("full_payload", "sem resultado")
            else:
                ably_state.last_gold = "Pipeline falhou ou sem dados"
                ably_state.last_error = "ValueError: source payload did not contain stream events (Silver/Gold vazios)"
        except Exception as e:
            ably_state.last_error = f"{type(e).__name__}: {str(e)}"
            
        ably_state.status = "Parado"
        ably_state.current_consumer = None
    return {"status": "Parado"}

@app.get("/api/ably/status")
def get_ably_status():
    return {
        "status": ably_state.status,
        "accepted": ably_state.accepted,
        "ignored": ably_state.ignored,
        "invalid": ably_state.invalid,
        "last_error": ably_state.last_error,
        "last_gold": ably_state.last_gold,
        "channel": ably_state.channel_name
    }

@app.post("/api/trigger")
def trigger_pipeline():
    base_dir = "data"
    bronze_proc = BronzeProcessor(base_dir)
    silver_proc = SilverProcessor(base_dir, bronze_proc)
    gold_proc = GoldProcessor(base_dir, silver_proc, api_url="http://127.0.0.1:8000/api/data")

    bronze_proc.clear()
    silver_proc.clear()
    gold_proc.clear()

    fragments = [
        {"part_index": 1, "payload": "Olá"},
        {"part_index": 2, "payload": "mundo"},
        {"part_index": 3, "payload": ", este é o enigma"},
        {"part_index": 4, "payload": "!"}
    ]

    duplicates = [
        {"part_index": 2, "payload": "mundo"}
    ]
    
    raw_stream = fragments + duplicates
    random.shuffle(raw_stream)

    consumer = ConsumerReceiver(bronze_proc)
    ingested_files = []
    for item in raw_stream:
        filepath = consumer.receive_message(item)
        ingested_files.append(filepath)

    silver_records, silver_file = silver_proc.process_bronze_data()
    gold_record = gold_proc.process_silver_data()
    api_success = gold_proc.send_to_api(gold_record)

    return {
        "status": "success",
        "gold": {
            "assembled_message": gold_record.get("full_payload", "") if gold_record else "sem resultado"
        }
    }

@app.get("/", response_class=HTMLResponse)
def get_dashboard():
    html_content = """
    <!DOCTYPE html>
    <html lang="pt-BR">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Enigma - Medallion Pipeline Dashboard</title>
        <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;800&family=Plus+Jakarta+Sans:wght@300;400;600;700&display=swap" rel="stylesheet">
        <style>
            :root {
                --bg-primary: #030712;
                --bg-secondary: #0f172a;
                --accent-blue: #3b82f6;
                --accent-cyan: #06b6d4;
                --color-bronze: #d97706;
                --color-silver: #94a3b8;
                --color-gold: #fbbf24;
                --color-success: #10b981;
                --text-main: #f3f4f6;
                --text-muted: #9ca3af;
                --glass-bg: rgba(15, 23, 42, 0.65);
                --glass-border: rgba(255, 255, 255, 0.08);
            }

            * { box-sizing: border-box; margin: 0; padding: 0; }

            body {
                font-family: 'Plus Jakarta Sans', sans-serif;
                background-color: var(--bg-primary);
                background-image: 
                    radial-gradient(circle at 10% 20%, rgba(59, 130, 246, 0.05) 0%, transparent 40%),
                    radial-gradient(circle at 90% 80%, rgba(6, 182, 212, 0.05) 0%, transparent 45%);
                background-attachment: fixed;
                color: var(--text-main);
                min-height: 100vh;
                padding: 2rem;
                display: flex;
                flex-direction: column;
                align-items: center;
            }

            header {
                width: 100%;
                max-width: 1200px;
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-bottom: 2rem;
                border-bottom: 1px solid var(--glass-border);
                padding-bottom: 1.5rem;
            }

            h1 {
                font-family: 'Outfit', sans-serif;
                font-weight: 800;
                font-size: 2.2rem;
                background: linear-gradient(135deg, #3b82f6, #06b6d4, #10b981);
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
                letter-spacing: -0.5px;
            }

            .subtitle { color: var(--text-muted); font-size: 0.95rem; margin-top: 0.25rem; }

            main {
                width: 100%;
                max-width: 1200px;
                display: flex;
                flex-direction: column;
                gap: 2rem;
            }

            .pipeline-flow {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
                gap: 1.5rem;
                width: 100%;
            }

            .flow-card {
                background: var(--glass-bg);
                border: 1px solid var(--glass-border);
                backdrop-filter: blur(12px);
                border-radius: 18px;
                padding: 1.5rem;
                text-align: center;
                position: relative;
                overflow: hidden;
            }

            .flow-card::before {
                content: ''; position: absolute; top: 0; left: 0; width: 100%; height: 4px; opacity: 0.8;
            }

            .card-producer::before { background: #f59e0b; }
            .card-consumer::before { background: var(--accent-cyan); }
            .card-bronze::before { background: var(--color-bronze); }
            .card-silver::before { background: var(--color-silver); }
            .card-gold::before { background: var(--color-gold); }
            .card-api::before { background: var(--color-success); }

            .layer-badge {
                font-size: 0.7rem; text-transform: uppercase; font-weight: 800;
                padding: 0.3rem 0.7rem; border-radius: 20px; margin-bottom: 1rem; display: inline-block;
            }

            .badge-producer { background: rgba(245, 158, 11, 0.15); color: #f59e0b; }
            .badge-consumer { background: rgba(6, 182, 212, 0.15); color: var(--accent-cyan); }
            .badge-bronze { background: rgba(217, 119, 6, 0.15); color: var(--color-bronze); }
            .badge-silver { background: rgba(148, 163, 184, 0.15); color: var(--color-silver); }
            .badge-gold { background: rgba(251, 191, 36, 0.15); color: var(--color-gold); }
            .badge-api { background: rgba(16, 185, 129, 0.15); color: var(--color-success); }

            .card-title { font-size: 1.15rem; font-weight: 700; margin-bottom: 0.5rem; }
            .card-desc { font-size: 0.8rem; color: var(--text-muted); line-height: 1.4; }

            .panel {
                background: var(--glass-bg);
                border: 1px solid var(--glass-border);
                backdrop-filter: blur(12px);
                border-radius: 24px;
                padding: 2rem;
                width: 100%;
            }

            .panel-title {
                font-family: 'Outfit', sans-serif; font-weight: 700; font-size: 1.3rem;
                margin-bottom: 1.5rem; display: flex; justify-content: space-between; align-items: center;
                border-bottom: 1px solid var(--glass-border); padding-bottom: 0.75rem;
            }

            /* Estilos do Bloco Ably */
            .ably-controls {
                display: flex;
                flex-wrap: wrap;
                gap: 1.5rem;
                align-items: flex-end;
                margin-bottom: 1.5rem;
            }

            .input-group {
                display: flex;
                flex-direction: column;
                gap: 0.5rem;
                flex: 1;
                min-width: 250px;
            }

            .input-group label {
                font-size: 0.8rem;
                text-transform: uppercase;
                letter-spacing: 1px;
                color: var(--text-muted);
                font-weight: 600;
            }

            .input-group input[type="text"], .input-group input[type="password"] {
                background: #0b1120;
                border: 1px solid #1e293b;
                color: white;
                padding: 0.8rem 1rem;
                border-radius: 12px;
                font-size: 0.95rem;
                font-family: monospace;
                transition: border-color 0.3s;
            }
            .input-group input:focus {
                outline: none;
                border-color: var(--accent-cyan);
            }

            .checkbox-group {
                display: flex;
                gap: 1.5rem;
                margin-bottom: 1.5rem;
            }

            .checkbox-label {
                display: flex;
                align-items: center;
                gap: 0.5rem;
                font-size: 0.85rem;
                font-weight: 600;
                color: var(--text-main);
                cursor: pointer;
            }

            .btn-start {
                background: linear-gradient(135deg, #0284c7, #06b6d4);
                color: #ffffff;
                border: none;
                padding: 0.8rem 2rem;
                border-radius: 12px;
                font-weight: 700;
                font-size: 0.95rem;
                cursor: pointer;
                display: flex;
                align-items: center;
                gap: 0.5rem;
                box-shadow: 0 4px 15px rgba(6, 182, 212, 0.3);
                transition: transform 0.2s;
            }
            .btn-start:hover { transform: translateY(-2px); }

            .btn-stop {
                background: #1e293b;
                color: #ffffff;
                border: 1px solid #334155;
                padding: 0.8rem 2rem;
                border-radius: 12px;
                font-weight: 700;
                font-size: 0.95rem;
                cursor: pointer;
                display: flex;
                align-items: center;
                gap: 0.5rem;
                transition: background 0.2s;
            }
            .btn-stop:hover { background: #334155; }

            .status-box {
                background: #070e1a;
                border: 1px solid #1e293b;
                border-radius: 12px;
                padding: 1.5rem;
                font-size: 0.9rem;
                line-height: 1.6;
            }

            .status-running { color: var(--color-success); font-weight: bold; }
            .status-stopped { color: var(--text-muted); font-weight: bold; }
            .status-error { color: #ef4444; margin-top: 0.5rem; }

            .run-table { width: 100%; border-collapse: collapse; margin-top: 1rem; text-align: left; }
            .run-table th { padding: 1rem; color: var(--text-muted); font-weight: 600; font-size: 0.85rem; text-transform: uppercase; border-bottom: 1px solid var(--glass-border); }
            .run-table td { padding: 1.2rem 1rem; border-bottom: 1px solid rgba(255, 255, 255, 0.04); font-size: 0.9rem; }
            .status-tag { display: inline-block; padding: 0.25rem 0.6rem; border-radius: 8px; font-size: 0.75rem; font-weight: 700; }
            .status-completed { background: rgba(16, 185, 129, 0.15); color: var(--color-success); border: 1px solid rgba(16, 185, 129, 0.3); }
            .status-incomplete { background: rgba(239, 68, 68, 0.15); color: #ef4444; border: 1px solid rgba(239, 68, 68, 0.3); }

            .toast {
                position: fixed; bottom: 2rem; right: 2rem; background: #1e293b; border: 1px solid var(--color-success);
                color: #fff; padding: 1rem 1.5rem; border-radius: 12px; box-shadow: 0 10px 25px rgba(0,0,0,0.3);
                transform: translateY(150%); transition: transform 0.4s; display: flex; align-items: center; gap: 0.75rem; z-index: 1000;
            }
            .toast.show { transform: translateY(0); }
        </style>
    </head>
    <body>
        <header>
            <div>
                <h1>Enigma Medallion Pipeline</h1>
                <div class="subtitle">Simulação local de arquitetura Medallion e streaming de dados</div>
            </div>
            <div>
                <button class="btn-stop" onclick="triggerManual()" style="background: rgba(16, 185, 129, 0.15); color: var(--color-success); border-color: rgba(16, 185, 129, 0.3);">
                    Execução Manual de Teste
                </button>
            </div>
        </header>

        <main>
            <section class="pipeline-flow">
                <div class="flow-card card-producer"><span class="layer-badge badge-producer">Streaming</span><div class="card-title">1. Producer</div><div class="card-desc">Simula o streaming enviando pacotes fora de ordem e duplicados.</div></div>
                <div class="flow-card card-consumer"><span class="layer-badge badge-consumer">Queue</span><div class="card-title">2. Consumer</div><div class="card-desc">Valida o payload básico e enfileira para ingestão imediata.</div></div>
                <div class="flow-card card-bronze"><span class="layer-badge badge-bronze">Bronze Layer</span><div class="card-title">3. Raw Landing</div><div class="card-desc">Armazena os dados brutos exatamente como chegaram. Formato JSON.</div></div>
                <div class="flow-card card-silver"><span class="layer-badge badge-silver">Silver Layer</span><div class="card-title">4. Clean & Sort</div><div class="card-desc">Remove duplicados pelo índice, ordena e limpa os campos de texto.</div></div>
                <div class="flow-card card-gold"><span class="layer-badge badge-gold">Gold Layer</span><div class="card-title">5. Aggregated</div><div class="card-desc">Junta as partes, valida a integridade sequencial e gera o resultado.</div></div>
                <div class="flow-card card-api"><span class="layer-badge badge-api">Rest API</span><div class="card-title">6. Integration</div><div class="card-desc">API final de consumo que recebe a mensagem unificada e consolidada.</div></div>
            </section>

            <!-- BLOCO DO ABLY -->
            <section class="panel">
                <div class="panel-title">
                    Canal Ably
                    <span style="font-size: 0.8rem; color: var(--text-muted); font-weight: normal;">Realtime</span>
                </div>
                
                <div class="ably-controls">
                    <div class="input-group" style="flex: 2;">
                        <label>ABLY API KEY</label>
                        <input type="password" id="ably-key" value="S-9Bpw.qPLA-Q:g9b8FZf8xP-MqrTK0GMhxrf1CC4GSMCbbxnNQG42qL4">
                    </div>
                    <div class="input-group">
                        <label>CANAL</label>
                        <input type="text" id="ably-channel" value="enigma-stream">
                    </div>
                    <button class="btn-start" onclick="startAbly()">
                        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polygon points="5 3 19 12 5 21 5 3"></polygon></svg>
                        Iniciar
                    </button>
                    <button class="btn-stop" onclick="stopAbly()">
                        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="6" y="6" width="12" height="12"></rect></svg>
                        Parar
                    </button>
                </div>

                <div class="checkbox-group">
                    <label class="checkbox-label">
                        <input type="checkbox" id="chk-limpar" checked style="accent-color: var(--accent-cyan);"> LIMPAR CAMADAS
                    </label>
                    <label class="checkbox-label">
                        <input type="checkbox" id="chk-dedup" checked style="accent-color: var(--accent-cyan);"> DEDUPLICAR ORIGEM
                    </label>
                </div>

                <div class="status-box">
                    <div>Status: <span id="lbl-status" class="status-stopped">Parado</span></div>
                    <div>Fonte: <span style="color: white;">Ably / <span id="lbl-channel">enigma-stream</span></span></div>
                    <div style="margin-top: 0.5rem;">Eventos aceitos: <span id="lbl-accepted" style="color: white;">0</span> · Duplicados ignorados: <span id="lbl-ignored" style="color: white;">0</span> · Inválidos: <span id="lbl-invalid" style="color: white;">0</span></div>
                    <div style="margin-top: 0.5rem;">Último Gold: <span id="lbl-gold" style="color: var(--color-gold);">sem resultado</span></div>
                    <div id="lbl-error" class="status-error" style="display: none;"></div>
                </div>
            </section>

            <section class="panel">
                <div class="panel-title">
                    Produtos de Dados Finais Recebidos na API (Camada Gold)
                    <span id="api-status-tag" class="status-tag status-completed" style="font-size: 0.7rem;">✔ external source runner is already running</span>
                </div>

                <table class="run-table">
                    <thead>
                        <tr><th>Run ID</th><th>Mensagem Consolidada (Gold final)</th><th>Partes</th><th>Integridade</th><th>Recebido em (API)</th></tr>
                    </thead>
                    <tbody id="runs-tbody">
                    </tbody>
                </table>
            </section>
        </main>

        <div id="toast" class="toast">
            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="#10b981" stroke-width="2.5"><polyline points="20 6 9 17 4 12"></polyline></svg>
            <span id="toast-message">Ação realizada!</span>
        </div>

        <script>
            async function startAbly() {
                const key = document.getElementById('ably-key').value;
                const chan = document.getElementById('ably-channel').value;
                const limpar = document.getElementById('chk-limpar').checked;
                const dedup = document.getElementById('chk-dedup').checked;

                await fetch('/api/ably/start', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ api_key: key, channel: chan, limpar_camadas: limpar, deduplicar_origem: dedup })
                });
                showToast("Conexão Ably iniciada.");
                pollStatus();
            }

            async function stopAbly() {
                await fetch('/api/ably/stop', { method: 'POST' });
                showToast("Conexão parada. Pipeline Medallion executado!");
                pollStatus();
                setTimeout(fetchRuns, 1000);
            }

            async function triggerManual() {
                await fetch('/api/trigger', { method: 'POST' });
                showToast("Pipeline executado manualmente!");
                setTimeout(fetchRuns, 500);
            }

            async function pollStatus() {
                try {
                    const res = await fetch('/api/ably/status');
                    const data = await res.json();
                    
                    const lblStatus = document.getElementById('lbl-status');
                    lblStatus.textContent = data.status;
                    if(data.status === "Rodando") {
                        lblStatus.className = "status-running";
                    } else if(data.status === "Erro") {
                        lblStatus.className = "status-error";
                    } else {
                        lblStatus.className = "status-stopped";
                    }

                    document.getElementById('lbl-channel').textContent = data.channel;
                    document.getElementById('lbl-accepted').textContent = data.accepted;
                    document.getElementById('lbl-ignored').textContent = data.ignored;
                    document.getElementById('lbl-invalid').textContent = data.invalid;
                    document.getElementById('lbl-gold').textContent = data.last_gold;

                    const errBox = document.getElementById('lbl-error');
                    if (data.last_error) {
                        errBox.style.display = 'block';
                        errBox.textContent = "Erro: " + data.last_error;
                    } else {
                        errBox.style.display = 'none';
                    }

                    if (data.status === "Rodando" || data.status === "Conectando...") {
                        setTimeout(pollStatus, 1500);
                    }
                } catch(e) {}
            }

            async function fetchRuns() {
                try {
                    const response = await fetch('/api/data');
                    const data = await response.json();
                    const tbody = document.getElementById('runs-tbody');
                    
                    if (data.length === 0) {
                        tbody.innerHTML = '<tr><td colspan="5" style="text-align:center; color:gray;">Nenhum dado recebido ainda.</td></tr>';
                        return;
                    }
                    
                    tbody.innerHTML = data.map(run => {
                        const date = new Date(run.received_at_api).toLocaleTimeString();
                        const isOk = run.data_integrity_status === 'COMPLETED';
                        return `
                            <tr>
                                <td style="font-weight: bold; font-family: monospace; color: var(--accent-cyan);">${run.pipeline_run_id}</td>
                                <td style="color: #ffffff; font-weight: 600;">"${run.full_payload}"</td>
                                <td>${run.assembled_parts_count} partes (${run.min_index} a ${run.max_index})</td>
                                <td><span class="status-tag ${isOk ? 'status-completed' : 'status-incomplete'}">${isOk ? '100% Integra' : 'Gaps detectados'}</span></td>
                                <td style="color: var(--text-muted);">${date}</td>
                            </tr>
                        `;
                    }).join('');
                } catch (e) {}
            }

            function showToast(message) {
                const toast = document.getElementById('toast');
                document.getElementById('toast-message').textContent = message;
                toast.classList.add('show');
                setTimeout(() => toast.classList.remove('show'), 3000);
            }

            fetchRuns();
            pollStatus();
            setInterval(fetchRuns, 3000);
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)
