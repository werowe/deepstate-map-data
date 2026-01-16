// --- CONFIGURATION ---
var collectionName = "geodata";    // Replace with your collection name
var fieldToIndex = "doc_hash_text";      // Replace with the field causing index violations
// ---------------------

var bulkOps = [];
var count = 0;

print(`Scanning '${collectionName}' for duplicates on field '${fieldToIndex}'...`);

// 1. Find all duplicates
db.getCollection(collectionName).aggregate([
    { 
        $group: { 
            _id: { [fieldToIndex]: "$" + fieldToIndex }, // Group by your target field
            uniqueIds: { $push: "$_id" },                // Collect all _ids
            count: { $sum: 1 } 
        }
    },
    { 
        $match: { 
            count: { $gt: 1 }                            // Only keep groups with duplicates
        }
    },
    {
        $sort: { _id: 1 }                                // Optional: Process in predictable order
    }
], { allowDiskUse: true })                               // Allow large datasets to run
.forEach(function(doc) {
    // 2. Determine which to keep
    // doc.uniqueIds is an array of _ids. 
    // Sorting ensure we keep the oldest (smallest ObjectId) and delete newer ones.
    // To keep the NEWEST instead, use: doc.uniqueIds.sort().reverse();
    
    doc.uniqueIds.sort(); 
    
    // Remove the first item (the one we keep) from the list of things to delete
    var idsToDelete = doc.uniqueIds.slice(1); 

    // 3. Prepare bulk delete operations
    idsToDelete.forEach(function(id) {
        bulkOps.push({
            deleteOne: {
                filter: { _id: id }
            }
        });
        count++;
    });

    // Execute in batches of 1000 to prevent memory issues
    if (bulkOps.length >= 1000) {
        db.getCollection(collectionName).bulkWrite(bulkOps);
        bulkOps = [];
        print("Purged batch...");
    }
});

// 4. Cleanup remaining
if (bulkOps.length > 0) {
    db.getCollection(collectionName).bulkWrite(bulkOps);
}

print("Finished! Removed " + count + " duplicate documents.");

