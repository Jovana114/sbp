db.product_with_feedbacks.createIndex({ "brand_name": 1 });
db.product_with_feedbacks.createIndex({ "_id": 1 });
db.product_with_feedbacks.createIndex({ "feedback.rating": 1 });
db.product_with_feedbacks.createIndex({ "tertiary_category": 1 });