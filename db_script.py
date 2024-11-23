import psycopg2

connection_params = {
    "dbname": "shop_db",
    "user": "user",
    "password": "password",
    "host": "localhost",
    "port": 5432
}

class Database:
    def __init__(self, connection_params):
        self.connection_params = connection_params

    def connect(self):
        try:
            self.conn = psycopg2.connect(**self.connection_params)
            self.cursor = self.conn.cursor()
            print("Підключено до бази даних.")
        except Exception as e:
            print("Помилка підключення:", e)

    def close(self):
        self.cursor.close()
        self.conn.close()

    def execute_query(self, query, params=None, fetch=False):
        try:
            self.cursor.execute(query, params)
            self.conn.commit()
            if fetch:
                return self.cursor.fetchall()
        except Exception as e:
            print("Помилка виконання запиту:", e)
            self.conn.rollback()

    def create_tables(self):
        self.execute_query("""
            CREATE TABLE IF NOT EXISTS clients (
                client_id SERIAL PRIMARY KEY,
                firm_name VARCHAR(100) NOT NULL,
                client_type VARCHAR(20) CHECK (client_type IN ('Юридична', 'Фізична')),
                address VARCHAR(150),
                phone VARCHAR(20),
                contact_person VARCHAR(100),
                account_number VARCHAR(30)
            );

            CREATE TABLE IF NOT EXISTS products (
                product_id SERIAL PRIMARY KEY,
                product_name VARCHAR(100) NOT NULL,
                price NUMERIC(10, 2) NOT NULL,
                quantity INT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS sales (
                sale_id SERIAL PRIMARY KEY,
                sale_date DATE NOT NULL,
                client_id INT REFERENCES clients(client_id) ON DELETE CASCADE,
                product_id INT REFERENCES products(product_id) ON DELETE CASCADE,
                quantity_sold INT NOT NULL,
                discount NUMERIC(4, 2) CHECK (discount BETWEEN 3 AND 20),
                payment_method VARCHAR(20) CHECK (payment_method IN ('Готівковий', 'Безготівковий')),
                delivery_needed BOOLEAN NOT NULL,
                delivery_cost NUMERIC(10, 2) NOT NULL
            );
        """)
        print("Таблиці успішно створені.")

    def insert_sample_data(self):
        clients_data = [
            ("ТОВ \"Клієнт-1\"", "Юридична", "вул. Першотравнева, 1", "111-222-3333", "Іван Іванов", "UA1234567890"),
            ("ФОП Петренко", "Фізична", "вул. Центральна, 5", "444-555-6666", "Петро Петренко", "UA9876543210")
        ]
        products_data = [
            ("Телефон", 12000.00, 50),
            ("Ноутбук", 30000.00, 20),
            ("Планшет", 15000.00, 30)
        ]
        sales_data = [
            ("2024-11-15", 1, 1, 2, 5, "Готівковий", True, 150.00),
            ("2024-11-16", 2, 2, 1, 10, "Безготівковий", False, 0.00),
            ("2024-11-17", 1, 3, 1, 15, "Готівковий", True, 100.00)
        ]

        for data in clients_data:
            self.execute_query("INSERT INTO clients (firm_name, client_type, address, phone, contact_person, account_number) VALUES (%s, %s, %s, %s, %s, %s)", data)
        for data in products_data:
            self.execute_query("INSERT INTO products (product_name, price, quantity) VALUES (%s, %s, %s)", data)
        for data in sales_data:
            self.execute_query("INSERT INTO sales (sale_date, client_id, product_id, quantity_sold, discount, payment_method, delivery_needed, delivery_cost) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", data)

        print("Зразкові дані додані.")

    def display_cash_sales(self):
        result = self.execute_query("""
            SELECT s.sale_id, c.firm_name, s.sale_date, p.product_name, s.quantity_sold, s.discount, s.payment_method
            FROM sales s
            JOIN clients c ON s.client_id = c.client_id
            JOIN products p ON s.product_id = p.product_id
            WHERE s.payment_method = 'Готівковий'
            ORDER BY c.firm_name;
        """, fetch=True)
        print("\nПродажі, оплачені готівкою:")
        for row in result:
            print(row)

    def display_sales_with_delivery(self):
        result = self.execute_query("""
            SELECT s.sale_id, c.firm_name, p.product_name, s.sale_date, s.delivery_needed, s.delivery_cost
            FROM sales s
            JOIN clients c ON s.client_id = c.client_id
            JOIN products p ON s.product_id = p.product_id
            WHERE s.delivery_needed = TRUE;
        """, fetch=True)
        print("\nПродажі з необхідністю доставки:")
        for row in result:
            print(row)

    def calculate_client_payments(self):
        result = self.execute_query("""
            SELECT c.firm_name, 
                   SUM(p.price * s.quantity_sold) AS total_amount,
                   SUM((p.price * s.quantity_sold) * (1 - s.discount / 100)) AS discounted_amount
            FROM sales s
            JOIN clients c ON s.client_id = c.client_id
            JOIN products p ON s.product_id = p.product_id
            GROUP BY c.firm_name;
        """, fetch=True)
        print("\nСума, яку треба сплатити кожному клієнту:")
        for row in result:
            print(row)

    def display_client_purchases(self, client_name):
        result = self.execute_query("""
            SELECT s.sale_id, p.product_name, s.sale_date, s.quantity_sold, s.discount
            FROM sales s
            JOIN clients c ON s.client_id = c.client_id
            JOIN products p ON s.product_id = p.product_id
            WHERE c.firm_name = %s;
        """, (client_name,), fetch=True)
        print(f"\nПокупки клієнта '{client_name}':")
        for row in result:
            print(row)

    def count_client_purchases(self):
        result = self.execute_query("""
            SELECT c.firm_name, COUNT(s.sale_id) AS purchase_count
            FROM sales s
            JOIN clients c ON s.client_id = c.client_id
            GROUP BY c.firm_name;
        """, fetch=True)
        print("\nКількість покупок кожного клієнта:")
        for row in result:
            print(row)

    def calculate_payments_by_method(self):
        result = self.execute_query("""
            SELECT c.firm_name, 
                   SUM(CASE WHEN s.payment_method = 'Готівковий' THEN (p.price * s.quantity_sold) * (1 - s.discount / 100) ELSE 0 END) AS cash_payment,
                   SUM(CASE WHEN s.payment_method = 'Безготівковий' THEN (p.price * s.quantity_sold) * (1 - s.discount / 100) ELSE 0 END) AS non_cash_payment
            FROM sales s
            JOIN clients c ON s.client_id = c.client_id
            JOIN products p ON s.product_id = p.product_id
            GROUP BY c.firm_name;
        """, fetch=True)
        print("\nСума, яку сплатив кожен клієнт (готівковий/безготівковий):")
        for row in result:
            print(row)

if __name__ == "__main__":
    db = Database(connection_params)
    db.connect()
    db.create_tables()
    db.insert_sample_data()
    db.display_cash_sales()
    db.display_sales_with_delivery()
    db.calculate_client_payments()
    db.display_client_purchases("ТОВ \"Клієнт-1\"")
    db.count_client_purchases()
    db.calculate_payments_by_method()
    db.close()
