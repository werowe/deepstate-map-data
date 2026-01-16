var cursor = db.geodata.aggregate([
    {
        "$group": {
            "_id": "$docs_hash_text",      // Group by the hash field
            "count": { "$sum": 1 },        // Count occurrences
            "ids": { "$push": "$_id" }     // List the _ids of the duplicates
        }
    },
    {
        "$match": {
            "count": { "$gt": 1 }          // Filter: show ONLY duplicates
        }
    },
    {
        "$project": {
            "_id": 0,                      // Hide the default _id field (optional)
            "duplicate_hash": "$_id",      // Rename _id to something readable
            "count": 1,
            "ids": 1                       // Show the list of duplicate _ids
        }
    }
])


cursor.forEach(printjson);

