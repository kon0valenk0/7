import psycopg2
from psycopg2 import sql

connection_params = {
    "dbname": "cinema_db",
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
            CREATE TABLE IF NOT EXISTS cinemas (
                cinema_id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                ticket_price NUMERIC(5, 2) NOT NULL,
                seats INT NOT NULL,
                address VARCHAR(150),
                phone VARCHAR(20)
            );
            CREATE TABLE IF NOT EXISTS movies (
                movie_id SERIAL PRIMARY KEY,
                title VARCHAR(100) NOT NULL,
                genre VARCHAR(50),
                duration INT NOT NULL,
                rating NUMERIC(3, 1)
            );
            CREATE TABLE IF NOT EXISTS screenings (
                screening_id SERIAL PRIMARY KEY,
                cinema_id INT REFERENCES cinemas(cinema_id) ON DELETE CASCADE,
                movie_id INT REFERENCES movies(movie_id) ON DELETE CASCADE,
                start_date DATE NOT NULL,
                show_days INT NOT NULL
            );
        """)
        print("Таблиці успішно створені.")

    def insert_sample_data(self):
        cinemas_data = [
            ("Кінотеатр Одеса", 150.00, 120, "вул. Дерибасівська, 18", "111-222-3333"),
            ("Кінотеатр Дніпро", 80.00, 90, "вул. Центральна, 45", "444-555-6666"),
            ("Кінотеатр Тернопіль", 95.00, 130, "вул. Степана Бандери, 4", "777-888-9999")
        ]
        movies_data = [
            ("Фільм А", "драма", 115, 8.2),
            ("Фільм Б", "фантастика", 140, 7.9),
            ("Фільм В", "історичний", 130, 8.5),
        ]
        
        for data in cinemas_data:
            self.execute_query("INSERT INTO cinemas (name, ticket_price, seats, address, phone) VALUES (%s, %s, %s, %s, %s)", data)
        
        for data in movies_data:
            self.execute_query("INSERT INTO movies (title, genre, duration, rating) VALUES (%s, %s, %s, %s)", data)

        print("Зразкові дані для кінотеатрів і фільмів додані.")

    def display_dramas(self):
        result = self.execute_query("SELECT * FROM movies WHERE genre = 'драма';", fetch=True)
        print("\nДрами:")
        for row in result:
            print(row)

    def sort_movies_by_duration(self):
        result = self.execute_query("SELECT * FROM movies ORDER BY duration ASC;", fetch=True)
        print("\nФільми, відсортовані за тривалістю:")
        for row in result:
            print(row)

    def calculate_screening_end_date(self):
        result = self.execute_query("""
            SELECT m.title, c.name, s.start_date, s.show_days,
                   s.start_date + s.show_days * INTERVAL '1 day' AS end_date
            FROM screenings s
            JOIN movies m ON s.movie_id = m.movie_id
            JOIN cinemas c ON s.cinema_id = c.cinema_id;
        """, fetch=True)
        print("\nДати кінцевого показу:")
        for row in result:
            print(row)

    def max_revenue_by_cinema(self):
        result = self.execute_query("""
            SELECT c.name, MAX(c.ticket_price * c.seats) AS max_revenue
            FROM cinemas c
            GROUP BY c.name;
        """, fetch=True)
        print("\nМаксимальний прибуток від одного показу для кожного кінотеатру:")
        for row in result:
            print(row)

    def display_movies_by_genre(self, genre):
        result = self.execute_query("SELECT * FROM movies WHERE genre = %s;", (genre,), fetch=True)
        print(f"\nФільми жанру '{genre}':")
        for row in result:
            print(row)

    def count_movies_by_genre(self):
        result = self.execute_query("SELECT genre, COUNT(*) FROM movies GROUP BY genre;", fetch=True)
        print("\nКількість фільмів за жанром:")
        for row in result:
            print(row)

    def display_all_tables(self):
        tables = ["cinemas", "movies", "screenings"]
        for table in tables:
            print(f"\nТаблиця {table}:")
            self.cursor.execute(f"SELECT * FROM {table};")
            columns = [desc[0] for desc in self.cursor.description]
            print(f"{' | '.join(columns)}")
            rows = self.cursor.fetchall()
            for row in rows:
                print(row)

if __name__ == "__main__":
    db = Database(connection_params)
    db.connect()
    db.create_tables()
    db.insert_sample_data()
    db.display_dramas()
    db.sort_movies_by_duration()
    db.calculate_screening_end_date()
    db.max_revenue_by_cinema()
    db.display_movies_by_genre("історичний")
    db.count_movies_by_genre()
    db.display_all_tables()
    db.close()
