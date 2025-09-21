
## Protocolo de comunicación (Client-Gateway)

### Envío de batch de líneas

#### Encabezado
| Campo              | Tamaño  | Descripción                                         |
| ------------------ | ------- | --------------------------------------------------- |
| `data type`      | 4 bytes | Tipo de datos que representados en el batch (entero, Big Endian)  
| `n`      | 4 bytes | Cantidad de lineas en el batch (entero, Big Endian)          |
| `last batch`      | 1 byte | Flag encendido si el batch es el último a enviar (booleano, comprimido)          |

#### Por línea en el batch
| Campo              | Tamaño  | Descripción                                         |
| ------------------ | ------- | --------------------------------------------------- |
| `m`      | 4 bytes | Cantidad de caracteres en la línea (entero, Big Endian) |
| `line`           | **m** bytes | Línea (UTF-8)                                      |         

### Respuesta de gateway
| Campo              | Tamaño  | Descripción                                         |
| ------------------ | ------- | --------------------------------------------------- |
| `response type`      | 4 bytes | Tipo de respuesta generado (entero, Big Endian)  |
| `m`      | 4 bytes | Cantidad de caracteres en la respuesta (entero, Big Endian) |
| `responseine`           | **m** bytes | Respuesta generada (UTF-8)  