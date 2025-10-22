#!/usr/bin/env python3
"""
Script para comparar archivos CSV result entre múltiples clientes.
Verifica que el contenido sea idéntico independientemente del orden de las filas.

Uso:
    python3 compare_csv.py                     # Compara 2 clientes por defecto
    python3 compare_csv.py 5                   # Compara 5 clientes
    python3 compare_csv.py --clients 3         # Compara 3 clientes
    python3 compare_csv.py --queries 1,2,3     # Solo compara queries específicas
"""

import csv
import os
import sys
import argparse
from pathlib import Path
from itertools import combinations

def read_csv_sorted(filepath):
    """Lee un CSV y lo ordena por todas las columnas para permitir comparación."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            rows = list(reader)
        
        # Ordenar las filas para hacer la comparación independiente del orden
        sorted_rows = sorted(rows)
        return sorted_rows
    except Exception as e:
        print(f"Error leyendo {filepath}: {e}")
        return None

def compare_csvs(file1, file2):
    """Compara dos archivos CSV independientemente del orden."""
    rows1 = read_csv_sorted(file1)
    rows2 = read_csv_sorted(file2)
    
    if rows1 is None or rows2 is None:
        return False, "Error al leer uno de los archivos", []
    
    # Verificar que tengan el mismo número de filas
    if len(rows1) != len(rows2):
        return False, f"Diferentes números de filas: {file1.name} tiene {len(rows1)}, {file2.name} tiene {len(rows2)}", []
    
    # Verificar que tengan el mismo número de columnas (si hay filas)
    if rows1 and rows2:
        if len(rows1[0]) != len(rows2[0]):
            return False, f"Diferentes números de columnas: {len(rows1[0])} vs {len(rows2[0])}", []
    
    # Comparar contenido y recopilar todas las diferencias
    try:
        differences = []
        for i, (row1, row2) in enumerate(zip(rows1, rows2)):
            if row1 != row2:
                differences.append({
                    'line_number': i + 1,
                    'file1_row': row1,
                    'file2_row': row2
                })
        
        if not differences:
            return True, "Archivos idénticos", []
        else:
            return False, f"Se encontraron {len(differences)} diferencias", differences
    
    except Exception as e:
        return False, f"Error comparando: {e}", []

def print_differences(differences, file1_name, file2_name):
    """Imprime todas las diferencias encontradas entre dos archivos."""
    if not differences:
        return
    
    print(f"\n  📋 DIFERENCIAS DETALLADAS entre {file1_name} y {file2_name}:")
    print(f"  {'='*60}")
    
    for i, diff in enumerate(differences):
        print(f"\n  Línea {diff['line_number']}:")
        print(f"    {file1_name}: {','.join(diff['file1_row'])}")
        print(f"    {file2_name}: {','.join(diff['file2_row'])}")
        
        if i >= 9:  # Mostrar máximo 10 diferencias para no saturar la salida
            remaining = len(differences) - i - 1
            if remaining > 0:
                print(f"\n  ... y {remaining} diferencias más")
            break

def find_client_files(current_dir, queries, num_clients):
    """Encuentra todos los archivos de clientes para las queries especificadas."""
    client_files = {}
    
    for query in queries:
        query_files = []
        for client in range(1, num_clients + 1):
            # Buscar diferentes patrones de nombres de archivo
            possible_patterns = [
                f"result_q{query}_client_client_{client}.csv",
                f"result_q{query} copy.csv" if client == 2 else f"result_q{query}.csv",
                f"result_q{query}_copy_{client}.csv",
                f"result_q{query}_{client}.csv"
            ]
            
            file_found = None
            for pattern in possible_patterns:
                file_path = current_dir / pattern
                if file_path.exists():
                    file_found = file_path
                    break
            
            if file_found:
                query_files.append((client, file_found))
            else:
                print(f"⚠️  No se encontró archivo para query {query}, cliente {client}")
        
        if len(query_files) >= 2:  # Necesitamos al menos 2 archivos para comparar
            client_files[query] = query_files
        else:
            print(f"❌ Query {query}: solo se encontraron {len(query_files)} archivos, se necesitan al menos 2")
    
    return client_files

def main():
    parser = argparse.ArgumentParser(description='Compara archivos CSV entre múltiples clientes')
    parser.add_argument('clients', nargs='?', type=int, default=2,
                        help='Número de clientes a comparar (default: 2)')
    parser.add_argument('--queries', type=str, default='1,2,3,4',
                        help='Queries a comparar, separadas por comas (default: 1,2,3,4)')
    parser.add_argument('--show-all-diffs', action='store_true',
                        help='Mostrar todas las diferencias detalladas')
    
    args = parser.parse_args()
    
    # Procesar parámetros
    num_clients = args.clients
    queries = [int(q.strip()) for q in args.queries.split(',')]
    show_all_diffs = args.show_all_diffs
    
    print(f"🔍 Comparando archivos CSV para {num_clients} clientes")
    print(f"📊 Queries a analizar: {queries}")
    if show_all_diffs:
        print("📋 Mostrando todas las diferencias detalladas")
    print()
    
    # Directorio actual
    current_dir = Path("/home/tomi/Escritorio/tp-distribuidos/data/received")
    
    # Encontrar archivos de clientes
    client_files = find_client_files(current_dir, queries, num_clients)
    
    if not client_files:
        print("❌ No se encontraron archivos para comparar")
        return 1
    
    all_match = True
    total_comparisons = 0
    
    # Comparar archivos para cada query
    for query, files in client_files.items():
        print(f"\n🔢 QUERY {query}")
        print("=" * 40)
        
        # Generar todas las combinaciones posibles de pares de clientes
        for (client1, file1), (client2, file2) in combinations(files, 2):
            total_comparisons += 1
            
            print(f"\nComparando Cliente {client1} vs Cliente {client2}")
            print(f"  📁 {file1.name} vs {file2.name}")
            
            # Mostrar información básica de los archivos
            try:
                size1 = os.path.getsize(file1)
                size2 = os.path.getsize(file2)
                print(f"  📏 Tamaños: {size1:,} bytes vs {size2:,} bytes")
            except:
                pass
            
            is_equal, message, differences = compare_csvs(file1, file2)
            
            if is_equal:
                print(f"  ✅ {message}")
            else:
                print(f"  ❌ {message}")
                all_match = False
                
                if show_all_diffs and differences:
                    print_differences(differences, f"Cliente_{client1}", f"Cliente_{client2}")
    
    # Resumen final
    print("\n" + "=" * 60)
    print(f"📊 RESUMEN: Se realizaron {total_comparisons} comparaciones")
    if all_match:
        print("🎉 RESULTADO: Todos los archivos CSV son idénticos entre todos los clientes!")
        return 0
    else:
        print("⚠️  RESULTADO: Se encontraron diferencias entre algunos clientes")
        print("💡 Tip: Usa --show-all-diffs para ver los detalles de las diferencias")
        return 1

if __name__ == "__main__":
    sys.exit(main())