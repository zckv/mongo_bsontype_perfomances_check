# mongo_bsontype_perfomances_check
Test indexion perfomances in mongodb for types Int, long, decimal, double, number, and no type
in validator. The document values inserted are python int on 32 bits.

Tests were run in a mongodb container started in replica set mode.

## Results 

### Find

Check find speed. As the documents are indexed on values, there is no
difference for finding speed.

![Find](/results/find_flat.png)

![Find Log](/results/find.png)


### Insertion

Check insertion speed. As the documents should be smaller for no_type, int and number, there should be
smaller insertion time.

From the results, it seems that insertion is the same speed for this kind of data.

![Insertion](/results/insertion_flat.png)

![Insertion Log](/results/insertion.png)

### Size

Check size in bytes. As the data is stored in more or less bytes, it should vary greatly in size.

From the results, it does vary but not greatly. MongoDB is compressing the data ?
Number, int, and no_type are the most optimized.

![Size](/results/size_flat.png)

![Size Log](/results/size.png)