
## Transacciones

### Encabezado
| Campo              | Tamaño  | Descripción                                         |
| ------------------ | ------- | --------------------------------------------------- |
| `n`      | 4 bytes | Cantidad de ítems en el batch de transacciones (entero, Big Endian)          |
| `last batch`      | 1 byte | Flag encendido si el batch es el último a enviar (booleano, comprimido)          |

### Por Ítem
| Campo              | Tamaño  | Descripción                                         |
| ------------------ | ------- | --------------------------------------------------- |
| `transaction id`   | 36 bytes | UUID de la transacción (UTF-8, `"xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"`)            |
| `store id`    | 4 bytes | Id de tienda (entero, Big Endian) |   
| `payment method id`    | 4 bytes | Id de método de pago (entero, Big Endian) |   
| `voucher id`    | 4 bytes | Id de voucher (entero, Big Endian) |   
| `user id`    | 4 bytes | Id de usuario (entero, Big Endian) |   
| `original amount`    | 8 bytes | Costo original (flotante, IEEE-754 Big Endian) |   
| `discount applied`    | 8 bytes | Descuento aplicado (flotante, IEEE-754 Big Endian) |   
| `final amount`    | 8 bytes | Costo final (flotante, IEEE-754 Big Endian) |
| `created at`   | 19 bytes | Fecha de creación (UTF-8, `"YYYY-MM-DD HH:MM:SS"`)

## Transacciones-Ítems

### Encabezado
| Campo              | Tamaño  | Descripción                                         |
| ------------------ | ------- | --------------------------------------------------- |
| `n`      | 4 bytes | Cantidad de ítems en el batch de transacciones-ítems (entero, Big Endian)          |
| `last batch`      | 1 byte | Flag encendido si el batch es el último a enviar (booleano, comprimido)          |

### Por Ítem
| Campo              | Tamaño  | Descripción                                         |
| ------------------ | ------- | --------------------------------------------------- |
| `transaction id`   | 36 bytes | UUID de la transacción (UTF-8, `"xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"`)            |
| `item id`    | 4 bytes | Id del ítem en cuestión (entero, Big Endian) |   
| `quantity`    | 4 bytes | Cantidad de ítems (entero, Big Endian) |   
| `unit price`    | 8 bytes | Costo individual (flotante, IEEE-754 Big Endian) |   
| `subtotal`    | 8 bytes | Costo del conjunto (flotante, IEEE-754 Big Endian) |   
| `created at`   | 19 bytes | Fecha de creación (UTF-8, `"YYYY-MM-DD HH:MM:SS"`)