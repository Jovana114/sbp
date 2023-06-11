import csv
from dateutil import parser
import pymongo

class SephoraParser:
    def __init__(self, file):
        self._file = file
        self.feedback = {}
        self.initialize()

    def read_feedback_from_csv(self, files):
        for file in files:
            with open(file, 'r', encoding='cp850') as csv_file:
                reader = csv.DictReader(csv_file)
                for row in reader:
                    if row['review_text'] != '':
                        product_id = row['product_id']
                        feedback = self.get_feedback(row)
                        if product_id not in self.feedback:
                            self.feedback[product_id] = [feedback]
                        else:
                            self.feedback[product_id].append(feedback)
    
    def initialize(self):
        files = ['data/reviews_0_250.csv', 'data/reviews_250_500.csv', 'data/reviews_500_750.csv', 
                 'data/reviews_750_1000.csv', 'data/reviews_1000_1500.csv', 'data/reviews_1500_end.csv']
        self.read_feedback_from_csv(files)


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

    def add_to_db(self, url, db_name):
        client = pymongo.MongoClient(url)
        db = client[db_name]
        with open(self._file, 'r', encoding='cp850') as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                product = get(row)
                product_id = row['product_id']
                if product_id in self.feedback:
                    feedback = self.feedback[product_id]
                    if product['_id'] == product_id:
                        product['feedback'] = feedback
                        db['product_info'].update_one({'_id': product_id}, {'$set': product}, upsert=True)
                else:
                    product['feedback'] = None
                    db['product_info'].update_one({'_id': product_id}, {'$set': product}, upsert=True)


def get(row) -> dict:
    return {
        '_id': row['product_id'],
        'product_name': row['product_name'],
        'brand_id': int(row['brand_id']),
        'brand_name': row['brand_name'],
        'loves_count': int(row['loves_count']),
        'rating': float(row['rating']) if row['rating'] != '' else 0.0,
        'reviews': row['reviews'],
        'ingredients': row['ingredients'].split(','),
        'price_usd': float(row['price_usd']),
        'limited_edition': int(row['limited_edition']),
        'new': int(row['new']),
        'online_only': int(row['online_only']),
        'highlights': row['highlights'].split(','),
        'primary_category': row['primary_category'].split(','),
        'tertiary_category': row['tertiary_category'].split(','),
        'feedback': []
    }

if __name__ == '__main__':
    print("Sephora reviews:")
    _parser = SephoraParser('data/product_info.csv')
    _parser.add_to_db(url='mongodb://localhost:27017/', db_name='sbp-opt')