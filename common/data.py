class Transaction:
    transaction_id: str
    store_id: int
    payment_method_id: int
    voucher_id: int
    user_id: int
    original_amount: float
    discount_applied: float
    final_amount: float
    created_at: str

class TransactionItem:
    transaction_id: str
    item_id: int
    quantity: int
    unit_price: float
    subtotal: float
    created_at: str