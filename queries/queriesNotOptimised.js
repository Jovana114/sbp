//Finding brand with max value_counts for every hair color, skin type and eye color
db.feedback.aggregate([
  {
    $lookup: {
      from: "product_info",
      localField: "product_id",
      foreignField: "_id",
      as: "product"
    }
  },
  {
    $unwind: "$product"
  },
  {
    $group: {
      _id: {
        hair_color: "$hair_color",
        skin_type: "$skin_type",
        eye_color: "$eye_color",
        brand_id: "$product.brand_id"
      },
      count: { $sum: 1 }
    }
  },
  {
    $sort: {
      "_id.hair_color": 1,
      "_id.skin_type": 1,
      "_id.eye_color": 1,
      count: -1
    }
  },
  {
    $group: {
      _id: {
        hair_color: "$_id.hair_color",
        skin_type: "$_id.skin_type",
        eye_color: "$_id.eye_color"
      },
      max_count: { $first: "$count" },
      brand_id: { $first: "$_id.brand_id" },
      brand: { $first: "$product.brand_name" }
    }
  },
  {
    $project: {
      _id: 0,
      hair_color: "$_id.hair_color",
      skin_type: "$_id.skin_type",
      eye_color: "$_id.eye_color",
      brand_id: 1,
      max_count: 1
    }
  },
  {
    $sort: {
      max_count: -1
    }
  }
]);

//Finding average rating for all feedbacks from 2022, which are recommended, for every product from tertiary_category and for every brand in it
db.feedback.aggregate([
  {
    $match: {
      submission_time: {
        $gte: ISODate("2022-01-01T00:00:00.000Z"),
        $lt: ISODate("2023-01-01T00:00:00.000Z")
      },
      is_recommended: "1.0"
    }
  },
  {
    $lookup: {
      from: "product_info",
      localField: "product_id",
      foreignField: "_id",
      as: "product"
    }
  },
  {
    $unwind: "$product"
  },
  {
    $match: {
      "product.tertiary_category": { $ne: "" },
      "product.brand_id": { $ne: NumberInt(0) }
    }
  },
  {
    $group: {
      _id: {
        tertiaryCategory: "$product.tertiary_category",
        brandId: "$product.brand_id",
        brandName: "$product.brand_name"
      },
      avgRating: {
        $avg: "$rating"
      }
    }
  },
  {
    $group: {
      _id: "$_id.tertiaryCategory",
      brands: {
        $push: {
          brandId: "$_id.brandId",
          brandName: "$_id.brandName",
          avgRating: "$avgRating"
        }
      }
    }
  },
  {
    $project: {
      _id: 0,
      tertiaryCategory: "$_id",
      brands: 1
    }
  },
  {
    $sort: {
      tertiaryCategory: 1
    }
  }
]);


//Finding profile of every person(hair_color, eye_color, skin_type) who left max number of positive feedbacks for every product from every categorie and every brand from them with limited editions 
db.product_info.aggregate([
  {
    $match: {
      limited_edition: 1
    }
  },
  {
    $lookup: {
      from: "feedback",
      localField: "_id",
      foreignField: "product_id",
      as: "feedbacks"
    }
  },
  {
    $unwind: "$feedbacks"
  },
  {
    $group: {
      _id: {
        product_id: "$_id",
        brand_name: "$brand_name"
      },
      profiles: {
        $push: {
          skin_color: "$feedbacks.skin_color",
          skin_type: "$feedbacks.skin_type",
          eye_color: "$feedbacks.eye_color",
          hair_color: "$feedbacks.hair_color",
          total_pos_feedback_count: "$feedbacks.total_pos_feedback_count"
        }
      }
    }
  },
  {
    $project: {
      _id: 0,
      product_id: "$_id.product_id",
      brand_name: "$_id.brand_name",
      profiles: {
        $reduce: {
          input: "$profiles",
          initialValue: { total_pos_feedback_count: 0 },
          in: {
            $cond: {
              if: { $gt: ["$$this.total_pos_feedback_count", "$$value.total_pos_feedback_count"] },
              then: "$$this",
              else: "$$value"
            }
          }
        }
      }
    }
  },
  {
    $group: {
      _id: "$product_id",
      brands: {
        $push: {
          brand_name: "$brand_name",
          profiles: "$profiles"
        }
      }
    }
  },
  {
    $sort: {
      "_id": 1
    }
  },
  {
    $group: {
      _id: null,
      products: {
        $push: {
          product_id: "$_id",
          brands: "$brands"
        }
      }
    }
  }
]);

//Finding brands with the cheapest products and total positive feedbacks count for every product from tertiary_category for all skin_tones and skin_types
db.feedback.aggregate([
  {
    $lookup: {
      from: "product_info",
      localField: "product_id",
      foreignField: "_id",
      as: "product_info"
    }
  },
  {
    $unwind: "$product_info"
  },
  {
    $match: {
      "product_info.tertiary_category": { $ne: "" },
      "skin_tone": { $ne: "" },
      "skin_type": { $ne: "" }
    }
  },
  {
    $group: {
      _id: {
        tertiary_category: "$product_info.tertiary_category",
        skin_tone: "$skin_tone",
        skin_type: "$skin_type"
      },
      min_price: { $min: "$product_info.price_usd" },
      brands: { $addToSet: "$product_info.brand_name" }
    }
  },
  {
    $project: {
      _id: 0,
      tertiary_category: "$_id.tertiary_category",
      skin_tone: "$_id.skin_tone",
      skin_type: "$_id.skin_type",
      min_price: 1,
      brands: 1
    }
  },
  {
    $sort: {
      min_price: 1,
      brands: 1
    }
  }
]);



//Finding products from all categories with max price for every rating from product_info
db.feedback.aggregate([
  {
    $lookup: {
      from: "product_info",
      localField: "product_id",
      foreignField: "_id",
      as: "product_info"
    }
  },
  {
    $unwind: "$product_info"
  },
  {
    $group: {
      _id: {
        rating: "$product_info.rating",
        brand: "$product_info.brand_name"
      },
      max_price: { $max: "$product_info.price_usd" },
      products: { $addToSet: "$product_info" }
    }
  },
  {
    $project: {
      _id: 0,
      rating: "$_id.rating",
      brand: "$_id.brand",
      product: {
        $filter: {
          input: "$products",
          as: "p",
          cond: { $eq: ["$$p.price_usd", "$max_price"] }
        }
      }
    }
  },
  {
    $unwind: "$product"
  },
  {
    $sort: {
      brand: -1
    }
  }
]);