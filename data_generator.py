from faker import Faker
import random
import pandas as pd  # Import pandas for data handling and CSV export
import os

class SyntheticDataGenerator:
    def __init__(self):
        self.fake = Faker('en_IN')  # Use Indian locale
        self.customers = []
        self.customer_addresses = []
        self.locations = []
        self.products = []
        self.delivery_partners = []
        self.orders = []
        self.order_items = []
        self.payments = []
        self.deliveries = []
        self.returns = []

        # Define cities with their respective states
        self.city_state_mapping = [
            ('Delhi', 'Delhi'),
            ('Jaipur', 'Rajasthan'),
            ('Chandigarh', 'Chandigarh'),
            ('Lucknow', 'Uttar Pradesh'),
            ('Bengaluru', 'Karnataka'),
            ('Chennai', 'Tamil Nadu'),
            ('Hyderabad', 'Telangana'),
            ('Kochi', 'Kerala'),
            ('Kolkata', 'West Bengal'),
            ('Bhubaneswar', 'Odisha'),
            ('Patna', 'Bihar'),
            ('Ranchi', 'Jharkhand'),
            ('Mumbai', 'Maharashtra'),
            ('Ahmedabad', 'Gujarat'),
            ('Pune', 'Maharashtra'),
            ('Goa', 'Goa')
        ]

    def generate_customers(self, num_records):
        for _ in range(num_records):
            customer = {
                'customer_id': _ + 1,
                'name': self.fake.name(),
                'gender': random.choice(['Male', 'Female']),
                'email': self.fake.email(),
                'dob': self.fake.date_of_birth(minimum_age=18, maximum_age=80),
                'phone_number': self.fake.phone_number(),
                'joining_date': self.fake.date_this_decade(),
            }
            self.customers.append(customer)

    def generate_locations(self):
        for location_id, (city, state) in enumerate(self.city_state_mapping, start=1):
            location = {
                'location_id': location_id,
                'city': city,
                'state': state,
                'country': 'India',
                'latitude': round(random.uniform(8.0, 37.0), 6),
                'longitude': round(random.uniform(68.0, 97.0), 6),
            }
            self.locations.append(location)

    def generate_customer_addresses(self, num_records):
        for _ in range(num_records):
            # Randomly pick a location from the existing locations
            location = random.choice(self.locations)
            
            address = {
                'address_id': _ + 1,
                'customer_id': random.choice(self.customers)['customer_id'],
                'address': self.fake.address(),  # You can replace this with a more specific address generator if needed
                'city': location['city'],
                'state': location['state'],
                'zip_code': random.randint(100000, 999999),
                'country': location['country'],
            }
            self.customer_addresses.append(address)

    def generate_products(self, num_records):
        categories = ['Electronics', 'Clothing', 'Groceries', 'Beverages']
        for _ in range(num_records):
            product = {
                'product_id': _ + 1,
                'product_name': self.fake.word(),
                'category': random.choice(categories),
                'price': round(random.uniform(100, 100), 2),
                'stock_quantity': random.randint(10, 100),
            }
            self.products.append(product)

    def generate_delivery_partners(self, num_records):
        for _ in range(num_records):
            partner = {
                'delivery_partner_id': _ + 1,
                'partner_name': self.fake.company(),
                'contact_number': self.fake.phone_number(),
                'service_area': random.choice(['North India', 'South India', 'East India', 'West India']),
            }
            self.delivery_partners.append(partner)

    def generate_orders(self, num_records):
        for _ in range(num_records):
            customer = random.choice(self.customers)
            address = random.choice(self.customer_addresses)
            order = {
                'order_id': _ + 1,
                'customer_id': customer['customer_id'],
                'order_date': self.fake.date_this_year(),
                'order_status': random.choice(['Pending', 'Shipped', 'Delivered', 'Cancelled']),
                'total_value': round(random.uniform(500, 20000), 2),
                'address_id': address['address_id'],
            }
            self.orders.append(order)

    def generate_order_items(self, num_records):
        for _ in range(num_records):
            order = random.choice(self.orders)
            product = random.choice(self.products)
            order_item = {
                'order_item_id': _ + 1,
                'order_id': order['order_id'],
                'product_id': product['product_id'],
                'quantity': random.randint(1, 3),
                'price_per_unit': product['price'],
            }
            self.order_items.append(order_item)

    def generate_payments(self, num_records):
        for _ in range(num_records):
            order = random.choice(self.orders)
            payment = {
                'payment_id': _ + 1,
                'order_id': order['order_id'],
                'payment_date': self.fake.date_this_year(),
                'payment_method': random.choice(['Credit Card', 'Debit Card', 'UPI', 'Cash on Delivery']),
                'payment_status': random.choice(['Completed', 'Failed', 'Pending']),
                'payment_amount': order['total_value'],
            }
            self.payments.append(payment)

    def generate_deliveries(self, num_records):
        for _ in range(num_records):
            order = random.choice(self.orders)
            delivery = {
                'delivery_id': _ + 1,
                'order_id': order['order_id'],
                'delivery_date': self.fake.date_this_year(),
                'delivery_status': random.choice(['Shipped', 'In Transit', 'Delivered']),
                'delivery_partner_id': random.choice(self.delivery_partners)['delivery_partner_id'],
                'tracking_number': self.fake.uuid4(),
            }
            self.deliveries.append(delivery)

    def generate_returns(self, num_records):
        for _ in range(num_records):
            order = random.choice(self.orders)
            return_item = {
                'return_id': _ + 1,
                'order_id': order['order_id'],
                'product_id': random.choice(self.order_items)['product_id'],
                'return_date': self.fake.date_this_year(),
                'return_reason': random.choice(['Damaged', 'Wrong Item', 'Not Satisfied']),
            }
            self.returns.append(return_item)

    def save_to_csv(self, output_dir='data'):
        # Ensure the output directory exists
        os.makedirs(output_dir, exist_ok=True)

        pd.DataFrame(self.customers).to_csv(f'{output_dir}/customers.csv', index=False)
        pd.DataFrame(self.customer_addresses).to_csv(f'{output_dir}/customer_addresses.csv', index=False)
        pd.DataFrame(self.locations).to_csv(f'{output_dir}/locations.csv', index=False)
        pd.DataFrame(self.products).to_csv(f'{output_dir}/products.csv', index=False)
        pd.DataFrame(self.delivery_partners).to_csv(f'{output_dir}/delivery_partners.csv', index=False)
        pd.DataFrame(self.orders).to_csv(f'{output_dir}/orders.csv', index=False)
        pd.DataFrame(self.order_items).to_csv(f'{output_dir}/order_items.csv', index=False)
        pd.DataFrame(self.payments).to_csv(f'{output_dir}/payments.csv', index=False)
        pd.DataFrame(self.deliveries).to_csv(f'{output_dir}/deliveries.csv', index=False)
        pd.DataFrame(self.returns).to_csv(f'{output_dir}/returns.csv', index=False)

if __name__ == '__main__':
    # Create an instance of the data generator
    data_generator = SyntheticDataGenerator()

    # Generate data
    data_generator.generate_customers(num_records=100)
    data_generator.generate_locations()
    data_generator.generate_customer_addresses(num_records=100)
    data_generator.generate_products(num_records=20)
    data_generator.generate_delivery_partners(num_records=5)
    data_generator.generate_orders(num_records=500)
    data_generator.generate_order_items(num_records=1000)
    data_generator.generate_payments(num_records=500)
    data_generator.generate_deliveries(num_records=500)
    data_generator.generate_returns(num_records=100)

    # Save data to CSV files
    data_generator.save_to_csv(output_dir='data')
