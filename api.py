from flask import Flask, jsonify
import os
from dotenv import load_dotenv
from flask_sqlalchemy import  SQLAlchemy
from flask_restful import Resource, Api, reqparse, fields, marshal_with, abort
import requests
from producer import publish_book_deleted_event, publish_book_created_event

load_dotenv()


FRONTEND_API_URL = 'http://applicatioalb-1070426547.us-east-1.elb.amazonaws.com' 

password= os.environ.get("DB_PASSWORD")
user=os.environ.get("DB_USER")
host=os.environ.get("DB_HOST")
db_name=os.environ.get("DB_NAME")

app  = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = f'postgresql://postgres:{password}@{host}:5432/{db_name}'
db = SQLAlchemy(app)
api =Api(app)

class BookModel(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(255), unique=True)
    author = db.Column(db.String(255),nullable=False)
    publisher = db.Column(db.String(100), nullable=False)
    category = db.Column(db.String(50), nullable=False)
   

    def __repr__(self):
        return f'<Book {self.title} by {self.author}>'
    

user_args=  reqparse.RequestParser()
user_args.add_argument('title', type=str, required=True,help="title cannot be blank")
user_args.add_argument('author', type=str, required=True,help="author cannot be blank")
user_args.add_argument('publisher', type=str, required=True,help="publisher cannot be blank")
user_args.add_argument('category', type=str, required=True,help="category cannot be blank")

userFields = {
    'id':  fields.Integer,
    'title': fields.String,
    'author': fields.String,
    'publisher':fields.String,
    'category':fields.String

}
class Books(Resource):
    @marshal_with(userFields)
    def post(self):
        args = user_args.parse_args()
        book = BookModel(title=args['title'],
                         author=args['author'],
                         publisher=args['publisher'],
                         category=args['category'])
        db.session.add(book)
        db.session.commit()
        book_data = {
                "id": book.id,
                "title": book.title,
                "author": book.author,
                "publisher": book.publisher,
                "category": book.category,
            }

        
        publish_book_created_event(book_data)
        
        all_books = BookModel.query.all()
        return all_books, 201

api.add_resource(Books,'/api/admin/book/')

class Book(Resource):
    @marshal_with(userFields)
    def delete(self, id):
        book = BookModel.query.filter_by(id=id).first()
        if not book:
            abort(404,"Book not found")
        db.session.delete(book)
        db.session.commit()
        book_data = {
                "id": book.id,
                "title": book.title,
                "author": book.author,
                "publisher": book.publisher,
                "category": book.category,
            }
        
        publish_book_deleted_event(book_data)
        all_books = BookModel.query.all()
        return all_books

api.add_resource(Book,'/api/admin/book/<int:id>')

@app.route('/api/admin/users', methods=['GET'])
def get_users():
    try:      
        response = requests.get(f'{FRONTEND_API_URL}/api/frontend/users/')
        response.raise_for_status()  
        users = response.json()
        return jsonify(users), 200
    except requests.exceptions.RequestException:
        return jsonify({"error": "Failed to fetch users from Frontend API"}), 500

@app.route('/api/admin/users-with-books', methods=['GET'])
def get_users_with_books():
    try:
        # Make a GET request to the  Frontend API's /users/books endpoint
        response = requests.get(f'{FRONTEND_API_URL}/api/frontend/users/books')
        response.raise_for_status()
        users_with_books = response.json()
        return jsonify(users_with_books), 200
    except requests.exceptions.RequestException:
        return jsonify({"error": "Failed to fetch users with books from Frontend API"}), 500

@app.route('/api/admin/borrowed-books', methods=['GET'])
def get_unavailable_books():
    try:
        # Make a GET request to the Django Frontend API's /books/borrowed endpoint
        response = requests.get(f'{FRONTEND_API_URL}/api/frontend/books/borrowed')
        response.raise_for_status()
        unavailable_books = response.json()
        return jsonify(unavailable_books), 200
    except requests.exceptions.RequestException:
        return jsonify({"error": "Failed to fetch unavailable books from Frontend API"}), 500


if __name__ == '__main__':
    app.run(debug=False)