# QueryOptimiser

The repository contains a program that can parse and optimise SQL queries by constructing left-deep trees and estimating the cost of each step. The following steps were done for query optimisation:

1. Start with canonical form
2. Move select operators  of the form *attr=value* down the tree
3. Reorder subtrees to put most restrictive selects first
4. Combine products and select to create joins (joins are more optimised  than product + select)
5. Move project operators down the tree to limit the intermediate relations

To run and test the program compile all java files under /src/sjdb/:

```bash
javac src/sjdb/*.java
```

And run the SJDB class file:

```bash
java src/sjdb/SJDB
```

Example queries can be found in the *data* directory.
