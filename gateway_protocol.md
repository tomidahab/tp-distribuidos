
## Protocolo de comunicación (Client-Gateway)

### Envío de batch de líneas

#### Encabezado
| Campo              | Tamaño  | Descripción                                         |
| ------------------ | ------- | --------------------------------------------------- |
| `n`      | 4 bytes | Cantidad de lineas en el batch (entero, Big Endian)          |
| `last batch`      | 1 byte | Flag encendido si el batch es el último a enviar (booleano, comprimido)          |

### Por línea en el batch
| Campo              | Tamaño  | Descripción                                         |
| ------------------ | ------- | --------------------------------------------------- |
| `m`      | 4 bytes | Cantidad de caracteres en la línea (entero, Big Endian) |
| `line`           | **m** bytes | Línea (UTF-8)                                      |         