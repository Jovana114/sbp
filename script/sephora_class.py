import pymongo
from pymongo import MongoClient
import csv
from dateutil import parser

class SephoraParser:
    def __init__(self, file):
        self._file = file

    def add_product_to_db(self, db):
        with open(self._file, 'r', encoding='cp850') as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                product = self.get_product(row)
                db['product_info'].update_one(
                    {'_id': product['_id']},
                    {'$set': product},
                    upsert=True
            )

    def get_product(self, row) -> dict:
        return {
            '_id': row['product_id'],
            'product_name': row['product_name'],
            'brand_id': row['brand_id'],
            'brand_name': row['brand_name'],
            'loves_count': int(row['loves_count']),
            'rating': row['rating'],
            'reviews': row['reviews'],
            'ingredients': row['ingredients'].split(','),
            'price_usd': float(row['price_usd']),
            'limited_edition': int(row['limited_edition']),
            'new': int(row['new']),
            'online_only': int(row['online_only']),
            'highlights': row['highlights'].split(','),
            'primary_category': row['primary_category'],
            'secondary_category': row['secondary_category'],
            'tertiary_category': row['tertiary_category']
        }

class SephoraParserReview:
    def __init__(self, files):
        self._files = files

    def add_feedback_to_db(self, db):
        for file in self._files:
            with open(file, 'r', encoding='cp850') as csv_file:
                reader = csv.DictReader(csv_file)
                for row in reader:
                    feedback = self.get_feedback(row)
                    try:
                        db['feedback'].insert_one(feedback)
                    except pymongo.errors.DuplicateKeyError:
                        continue

    def get_feedback(self, row) -> dict:
        return {
            '_id': row['author_id'],
            'rating': int(row['rating']),
            'is_recommended': row['is_recommended'],
            'total_neg_feedback_count': int(row['total_neg_feedback_count']),
            'total_pos_feedback_count': int(row['total_pos_feedback_count']),
            'submission_time': parser.parse(row['submission_time']),
            'review_text': row['review_text'],
            'skin_tone': row['skin_tone'],
            'eye_color': row['eye_color'],
            'skin_type': row['skin_type'],
            'hair_color': row['hair_color'],
            'product_id': row['product_id']
        }

if __name__ == '__main__':
    client = MongoClient('mongodb://localhost:27017/')
    db = client['sbp']

    print("Sephora products:")
    _parser = SephoraParser('data/product_info.csv')
    _parser.add_product_to_db(db)

    print("Sephora reviews:")
    files = [
        'data/reviews_0_250.csv',
        'data/reviews_250_500.csv',
        'data/reviews_500_750.csv',
        'data/reviews_750_1000.csv',
        'data/reviews_1000_1500.csv',
        'data/reviews_1500_end.csv'
    ]
    _parser_review = SephoraParserReview(files)
    _parser_review.add_feedback_to_db(db)