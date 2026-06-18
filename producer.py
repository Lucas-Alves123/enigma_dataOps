import requests
import json
import time

def main():
    print("================================================================================")
    print("          SIMULADOR DO PROFESSOR (PRODUTOR LOCAL - BYPASS ABLY)                 ")
    print("================================================================================")
    
    pecas_do_enigma = [
        {"part_index": 3, "payload": "arquitetura"},
        {"part_index": 1, "payload": "Dominando a"},
        {"part_index": 5, "payload": "DataOps!"},
        {"part_index": 2, "payload": "complexidade da"},
        {"part_index": 4, "payload": "com"}
    ]
    
    print("[Produtor] Preparando para enviar as peças direto para o Dashboard...")
    time.sleep(1)
    
    for peca in pecas_do_enigma:
        print(f"[Produtor] Enviando peça {peca['part_index']}: '{peca['payload']}'")
        try:
            response = requests.post("http://127.0.0.1:8000/api/ably/mock_produce", json=peca)
            if response.status_code == 200 and response.json().get("status") == "ok":
                pass
            else:
                print(f"[Aviso] {response.json().get('status')}")
        except Exception as e:
            print(f"[Erro] Falha ao enviar para o Dashboard. A API está rodando? Erro: {e}")
        
        time.sleep(1.5)
        
    print("--------------------------------------------------------------------------------")
    print("[Produtor] Todas as mensagens foram enviadas!")
    print("================================================================================")

if __name__ == "__main__":
    main()
