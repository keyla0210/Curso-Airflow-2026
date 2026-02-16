import requests
import json
import os
from datetime import datetime
from pathlib import Path

def ingest_raw(output_dir, timeout=15, **kwargs):
    """
    Tarea: Extract (Raw)
    Recibe 'output_dir' desde el DAG para saber dónde guardar.
    """
    api_url = "https://fakestoreapi.com/products"
    
    # 1. Crear la ruta completa del archivo
    # Usamos Path para manejar mejor las barras / en Linux/Docker
    folder_path = Path(output_dir)
    file_name = "products_raw.json" # Simplificado para que Bronze lo encuentre fácil
    full_path = folder_path / file_name

    # 2. Asegurar que la carpeta exista
    folder_path.mkdir(parents=True, exist_ok=True)

    try:
        print(f"Conectando a la fuente: {api_url}")
        response = requests.get(api_url, timeout=timeout)
        response.raise_for_status()
        
        data = response.json()

        # 3. Persistencia
        with open(full_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
            
        print(f"Éxito: Datos almacenados en {full_path}")
        print(f"Registros capturados: {len(data)}")
        
        return str(full_path)

    except Exception as e:
        print(f"Fallo en la ingesta: {str(e)}")
        raise