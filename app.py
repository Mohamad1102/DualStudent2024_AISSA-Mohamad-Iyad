import os
import pandas as pd
from flask import Flask, jsonify, request, render_template
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.exc import SQLAlchemyError
from werkzeug.exceptions import InternalServerError

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///salesdata.db'
db = SQLAlchemy(app)


class CustomerSales(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    customer_id = db.Column(db.String(20))
    first_name = db.Column(db.String(100))
    last_name = db.Column(db.String(100))
    company = db.Column(db.String(100))
    city = db.Column(db.String(100))
    country = db.Column(db.String(100))
    phone1 = db.Column(db.String(20))
    phone2 = db.Column(db.String(20))
    email = db.Column(db.String(100))
    subscription_date = db.Column(db.String(20))
    website = db.Column(db.String(100))
    sales_2021 = db.Column(db.Integer)
    sales_2022 = db.Column(db.Integer)


@app.route('/api/data', methods=['GET'])
def get_data():
    try:
        data = [{
            'customer_name': customer.first_name + ' ' + customer.last_name,  # Concatenate names
            'sales_2021': customer.sales_2021,
            'sales_2022': customer.sales_2022
        } for customer in CustomerSales.query.all()]
        return jsonify(data)
    except SQLAlchemyError as e:
        app.logger.error(f"Database error: {e}")
        raise InternalServerError("Database error, unable to retrieve data.")


@app.route('/api/sort_filter', methods=['GET'])
def sort_filter_data():
    sort_by = request.args.get('sort_by', 'id')  
    order = request.args.get('order', 'asc') 
    try:
        attribute = getattr(CustomerSales, sort_by, None)
        if attribute is None:
            return jsonify({'error': 'Invalid sort field'}), 400
        if order == 'asc':
            query = CustomerSales.query.order_by(attribute.asc())
        else:
            query = CustomerSales.query.order_by(attribute.desc())
        filtered_data = [{
            'customer_name': customer.first_name + ' ' + customer.last_name,
            'sales_2021': customer.sales_2021,
            'sales_2022': customer.sales_2022
        } for customer in query.all()]
        return jsonify(filtered_data)
    except SQLAlchemyError as e:
        app.logger.error(f"Database error during sorting/filtering: {e}")
        raise InternalServerError("Database error, unable to sort/filter data.")


@app.route('/api/charts', methods=['GET'])
def chart_data():
    try:
        pie_chart_data = db.session.query(
            CustomerSales.customer_id,
            db.func.sum(CustomerSales.sales_2022).label('total_sales_2022')
        ).group_by(CustomerSales.customer_id).all()

        line_chart_data = db.session.query(
            CustomerSales.customer_id,
            db.func.sum(CustomerSales.sales_2021).label('total_sales_2021'),
            db.func.sum(CustomerSales.sales_2022).label('total_sales_2022')
        ).group_by(CustomerSales.customer_id).all()

        return jsonify({
            'pie_chart_data': [{'customer_id': result.customer_id, 'total_sales_2022': float(result.total_sales_2022)}
                               for result in pie_chart_data],
            'line_chart_data': [{'customer_id': result.customer_id,
                                 'sales_data': {'2021': float(result.total_sales_2021),
                                                '2022': float(result.total_sales_2022)}} for result in line_chart_data]
        })
    except SQLAlchemyError as e:
        app.logger.error(f"Database error during chart data generation: {e}")
        raise InternalServerError("Database error, unable to generate chart data.")


@app.before_first_request
def create_tables_and_import_data():
    db.create_all()
    csv_file_path = os.path.join(os.getcwd(), 'data', 'customers_sales_2021_2022.csv')
    try:
        data = pd.read_csv(csv_file_path, delimiter=';', quoting=3, on_bad_lines='skip')
        for _, row in data.iterrows():
            customer_data = CustomerSales(
                customer_id=row['Customer Id'],
                first_name=row['First Name'],
                last_name=row['Last Name'],
                company=row['Company'],
                city=row['City'],
                country=row['Country'],
                phone1=row['Phone 1'],
                phone2=row['Phone 2'],
                email=row['Email'],
                subscription_date=row['Subscription Date'],
                website=row['Website'],
                sales_2021=int(row['SALES 2021']),
                sales_2022=int(row['SALES 2022'])
            )
            db.session.add(customer_data)
        db.session.commit()
    except Exception as e:
        app.logger.error(f"Error loading data from CSV: {e}")
        raise InternalServerError("Error initializing database with CSV data.")


@app.route('/')
def index():
    return render_template('index.html')


if __name__ == '__main__':
    app.run(debug=False)
