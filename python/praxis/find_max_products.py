transactions = [
    {'user_id': 'user1', 'product_id': 'A', 'quantity': 2},
    {'user_id': 'user2', 'product_id': 'B', 'quantity': 1},
    {'user_id': 'user1', 'product_id': 'A', 'quantity': 3},
    {'user_id': 'user3', 'product_id': 'C', 'quantity': 5},
    {'user_id': 'user2', 'product_id': 'C', 'quantity': 2},
]

def find_sum_of_products(transactions):
    product_quantities = {}
    for each_transaction in transactions:
        product_id = each_transaction['product_id']
        quantity = each_transaction['quantity']
        if product_id in product_quantities:
            product_quantities[product_id] = product_quantities[product_id] + quantity
        else:
            product_quantities[product_id] = quantity
    return product_quantities

def find_max(transactions):
    max_quantities = {}
    for each_tx in transactions:
        product = each_tx['product_id']
        qty = each_tx['quantity']
        if product not in max_quantities:
            max_quantities[product] = qty 
        else: 
            if qty > max_quantities[product]:
                max_quantities[product] = qty
    return max_quantities

print(find_sum_of_products(transactions))
print(find_max(transactions))

