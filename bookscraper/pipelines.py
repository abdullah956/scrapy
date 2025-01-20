import sqlite3
from itemadapter import ItemAdapter

class BookscraperPipeline:
    def open_spider(self, spider):
        self.connection = sqlite3.connect('scraped_books.db')  # Database file
        self.cursor = self.connection.cursor()
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS books (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT,
                title TEXT,
                upc TEXT,
                product_type TEXT,
                price_excl_tax REAL,
                price_incl_tax REAL,
                tax REAL,
                availability INTEGER,
                num_reviews INTEGER,
                stars INTEGER,
                category TEXT,
                description TEXT,
                price REAL
            )
        """)
        self.connection.commit()

    def close_spider(self, spider):
        self.connection.close()

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        print("Processing item:", adapter.asdict())
        # Process and clean the item
        field_names = adapter.field_names()
        for field_name in field_names:
            value = adapter.get(field_name)
            if isinstance(value, (list, tuple)):
                adapter[field_name] = value[0].strip() if value else None
            elif isinstance(value, str):
                adapter[field_name] = value.strip()

        # Convert to lowercase for specific fields
        lowercase_keys = ['category', 'product_type']
        for lowercase_key in lowercase_keys:
            value = adapter.get(lowercase_key)
            if isinstance(value, str):
                adapter[lowercase_key] = value.lower()

        # Convert prices to float
        price_keys = ['price', 'price_excl_tax', 'price_incl_tax', 'tax']
        for price_key in price_keys:
            value = adapter.get(price_key)
            if value:
                value = value.replace('£', '')
                adapter[price_key] = float(value)

        # Extract number of books in stock
        availability_string = adapter.get('availability')
        if availability_string:
            split_string_array = availability_string.split('(')
            if len(split_string_array) < 2:
                adapter['availability'] = 0
            else:
                availability_array = split_string_array[1].split(' ')
                adapter['availability'] = int(availability_array[0])

        # Convert reviews to integer
        num_reviews_string = adapter.get('num_reviews')
        if num_reviews_string:
            adapter['num_reviews'] = int(num_reviews_string)

        # Convert stars to integer
        stars_string = adapter.get('stars')
        if stars_string:
            split_stars_array = stars_string.split(' ')
            stars_text_value = split_stars_array[1].lower()
            stars_mapping = {
                "zero": 0, "one": 1, "two": 2,
                "three": 3, "four": 4, "five": 5
            }
            adapter['stars'] = stars_mapping.get(stars_text_value, 0)

        # Insert the cleaned item into the database
        self.cursor.execute("""
            INSERT INTO books (
                url, title, upc, product_type, price_excl_tax, 
                price_incl_tax, tax, availability, num_reviews, 
                stars, category, description, price
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            adapter.get('url'),
            adapter.get('title'),
            adapter.get('upc'),
            adapter.get('product_type'),
            adapter.get('price_excl_tax'),
            adapter.get('price_incl_tax'),
            adapter.get('tax'),
            adapter.get('availability'),
            adapter.get('num_reviews'),
            adapter.get('stars'),
            adapter.get('category'),
            adapter.get('description') if isinstance(adapter.get('description'), str) else None,
            adapter.get('price')
        ))
        self.connection.commit()

        return item
