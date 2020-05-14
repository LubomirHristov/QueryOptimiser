package sjdb;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("Duplicates")
public class Optimiser {
    private Catalogue catalogue;

    public Optimiser(Catalogue catalogue){
        this.catalogue = catalogue;
    }

    public Operator optimise(Operator op){
        // Get all operators from the canonical plan
        List<Operator> operators = getOperators(op, new ArrayList<>());

        // Separate each operator type
        List<Scan> scanList = new ArrayList<>();
        List<Product> productList = new ArrayList<>();
        List<Select> selectList = new ArrayList<>();
        List<Project> projectList = new ArrayList<>();

        for(Operator operator: operators){
            if(operator instanceof Scan){
                scanList.add((Scan) operator);
            }else if(operator instanceof Product){
                productList.add((Product) operator);
            }else if(operator instanceof Select){
                selectList.add((Select) operator);
            }else if(operator instanceof Project){
                projectList.add((Project) operator);
            }
        }

        List<Operator> optimisedScanList = new ArrayList<>();

        // Move selects in the form attr=val above the corresponding Relations
        for(Scan scan: scanList){
            Operator optimisedScan = scan;

            for(Select select: selectList){
                if(select.getPredicate().equalsValue() && scan.getRelation().getAttributes().contains(select.getPredicate().getLeftAttribute())){
                    optimisedScan = new Select(optimisedScan, select.getPredicate());
                }
            }

            optimisedScanList.add(optimisedScan);
        }

        // Reorder joins


        List<Operator> optimisedProductList = new ArrayList<>();

        // Make all possible pairs for product 1
        List<List<Operator>> pairs = new ArrayList<>();

        for(int i = 0; i < optimisedScanList.size() - 1; i++){
            for(int j = i + 1; j < optimisedScanList.size(); j++){
                List<Operator> pair = new ArrayList<>();
                pair.add(optimisedScanList.get(i));
                pair.add(optimisedScanList.get(j));
                pairs.add(pair);
            }
        }

        // Get all restricting attr in order to move Project operators
        List<Attribute> selectAndProjectAttributes = new ArrayList<>();

        for(Select select: selectList){
            if(!select.getPredicate().equalsValue()){
                if(!selectAndProjectAttributes.contains(select.getPredicate().getLeftAttribute())){
                    selectAndProjectAttributes.add(select.getPredicate().getLeftAttribute());
                }

                if(!selectAndProjectAttributes.contains(select.getPredicate().getRightAttribute())){
                    selectAndProjectAttributes.add(select.getPredicate().getRightAttribute());
                }
            }
        }

        for(Project project: projectList){
            for(Attribute attr: project.getAttributes()){
                if(!selectAndProjectAttributes.contains(attr)){
                    selectAndProjectAttributes.add(attr);
                }
            }
        }

        // Create all products

        for(int p = 0; p < productList.size(); p++){
            if(p == 0){
                // Create the best first product

                Operator bestProduct = null;
                int bestOutputSize = Integer.MAX_VALUE;
                List<Operator> inputsUsed = new ArrayList<>();

                // The attributes of a join should be removed after the join from the list with possible attributes to project
                Attribute leftPredicate = null;
                Attribute rightPredicate = null;

                // Estimate cost of product for each pair of inputs
                for(List<Operator> pair: pairs){
                    Operator product = new Product(pair.get(0), pair.get(1));
                    List<Attribute> productAttributes = getAttributes(product, new ArrayList<>());

                    Operator left = ((Product) product).getLeft();
                    Operator right = ((Product) product).getRight();

                    List<Attribute> leftAttributes = getAttributes(left, new ArrayList<>());
                    List<Attribute> rightAttributes = getAttributes(right, new ArrayList<>());

                    boolean leftCanBeOptimised = false;
                    boolean rightCanBeOptimised = false;

                    List<Attribute> leftProjectAttributes = new ArrayList<>();
                    List<Attribute> rightProjectAttributes = new ArrayList<>();

                    // Check if a Project can be put above each child node to limit size
                    for(Attribute attr: selectAndProjectAttributes){
                        if(leftAttributes.contains(attr)){
                            leftProjectAttributes.add(attr);
                            leftCanBeOptimised = true;
                        }else if(rightAttributes.contains(attr)){
                            rightProjectAttributes.add(attr);
                            rightCanBeOptimised =  true;
                        }
                    }

                    // if a project can be put and the project does not contain all possible attributes of the child node
                    // and if it is not the case that all attributes are projected in the end
                    if(leftCanBeOptimised && leftAttributes.size() != leftProjectAttributes.size() && !projectList.isEmpty()){
                        left = new Project(left, leftProjectAttributes);
                    }

                    if(rightCanBeOptimised && rightAttributes.size() != rightProjectAttributes.size() && !projectList.isEmpty()){
                        right = new Project(right, rightProjectAttributes);
                    }

                    // Match select in the form attr=attr with product to estimate cost
                    for(Select select: selectList){

                        // if a select can be put on top of the product
                        if(!select.getPredicate().equalsValue() && productAttributes.contains(select.getPredicate().getLeftAttribute()) && productAttributes.contains(select.getPredicate().getRightAttribute())){

                            // make sure the predicate is not reversed
                            Predicate predicate = leftAttributes.contains(select.getPredicate().getLeftAttribute()) ? select.getPredicate() : new Predicate(select.getPredicate().getRightAttribute(), select.getPredicate().getLeftAttribute());

                            if(product instanceof Join){
                                product = new Select(product, predicate);
                            }else{
                                product = new Join(left, right, predicate);
                                leftPredicate = predicate.getLeftAttribute();
                                rightPredicate = predicate.getRightAttribute();
                            }
                        }
                    }

                    Estimator estimator = new Estimator();
                    product.accept(estimator);

                    // if the cost of the current relation is less than the best one so far, make the current one best
                    if(product.getOutput().getTupleCount() < bestOutputSize){
                        bestProduct = product;
                        bestOutputSize = product.getOutput().getTupleCount();
                        inputsUsed = pair;
                        selectAndProjectAttributes.remove(leftPredicate);
                        selectAndProjectAttributes.remove(rightPredicate);
                    }
                }

                bestProduct.setOutput(null);
                optimisedProductList.add(bestProduct);

                // The used inputs should be removed from the possible inputs to create a future join/product
                optimisedScanList.remove(inputsUsed.get(0));
                optimisedScanList.remove(inputsUsed.get(1));
            }else{
                // for every next product match previous product with the best relation

                Operator bestProduct = null;
                int bestOutputSize = Integer.MAX_VALUE;
                Operator inputUsed = null;
                Attribute leftPredicate = null;
                Attribute rightPredicate = null;


                for(int i = 1; i < productList.size(); i++){
                    for(Operator optimisedScan: optimisedScanList){
                        Operator product = new Product(optimisedProductList.get(optimisedProductList.size() - 1), optimisedScan);
                        inputUsed = optimisedScan;
                        List<Attribute> productAttributes = getAttributes(product, new ArrayList<>());

                        Operator left = product.getInputs().get(0);
                        Operator right = product.getInputs().get(1);

                        List<Attribute> leftAttributes = getAttributes(left, new ArrayList<>());
                        List<Attribute> rightAttributes = getAttributes(right, new ArrayList<>());

                        boolean leftCanBeOptimised = false;
                        boolean rightCanBeOptimised = false;

                        List<Attribute> leftProjectAttributes = new ArrayList<>();
                        List<Attribute> rightProjectAttributes = new ArrayList<>();

                        // Move projects
                        for(Attribute attr: selectAndProjectAttributes){
                            if(leftAttributes.contains(attr)){
                                leftProjectAttributes.add(attr);
                                leftCanBeOptimised = true;
                            }else if(rightAttributes.contains(attr)){
                                rightProjectAttributes.add(attr);
                                rightCanBeOptimised =  true;
                            }
                        }

                        if(leftCanBeOptimised && leftAttributes.size() != leftProjectAttributes.size() && !projectList.isEmpty()){
                            left = new Project(left, leftProjectAttributes);
                        }

                        if(rightCanBeOptimised && rightAttributes.size() != rightProjectAttributes.size() && !projectList.isEmpty()){
                            right = new Project(right, rightProjectAttributes);
                        }

                        // Match select in the form attr=attr with product to estimate cost
                        for(Select select: selectList){
                            if(!select.getPredicate().equalsValue() && productAttributes.contains(select.getPredicate().getLeftAttribute()) && productAttributes.contains(select.getPredicate().getRightAttribute())){
                                // make sure the predicate is not reversed
                                Predicate predicate = leftAttributes.contains(select.getPredicate().getLeftAttribute()) ? select.getPredicate() : new Predicate(select.getPredicate().getRightAttribute(), select.getPredicate().getLeftAttribute());

                                if(product instanceof Join){
                                    product = new Select(product, predicate);
                                }else{
                                    product = new Join(left, right, predicate);
                                    leftPredicate = predicate.getLeftAttribute();
                                    rightPredicate = predicate.getRightAttribute();
                                }
                            }
                        }

                        Estimator estimator = new Estimator();
                        product.accept(estimator);

                        if(product.getOutput().getTupleCount() < bestOutputSize){
                            bestProduct = product;
                            bestOutputSize = product.getOutput().getTupleCount();
                            selectAndProjectAttributes.remove(leftPredicate);
                            selectAndProjectAttributes.remove(rightPredicate);
                        }
                    }

                    bestProduct.setOutput(null);
                    optimisedProductList.add(bestProduct);
                    optimisedScanList.remove(inputUsed);
                }
            }
        }

        Operator optimisedPlan = optimisedProductList.isEmpty() ? optimisedScanList.get(0) : optimisedProductList.get(optimisedProductList.size() - 1);

        for(Project project: projectList){
            optimisedPlan = new Project(optimisedPlan, project.getAttributes());
        }

        return optimisedPlan;
    }

    private List<Attribute> getAttributes(Operator op, List<Attribute> result){
        if(op instanceof Scan){
            for(Attribute attr: ((Scan) op).getRelation().getAttributes()){
                result.add(attr);
            }

            return result;
        }

        if(op instanceof Product){
            result = getAttributes(((Product) op).getLeft(), result);
            result = getAttributes(((Product) op).getRight(), result);
        }else if(op instanceof Join){
            result = getAttributes(((Join) op).getLeft(), result);
            result = getAttributes(((Join) op).getRight(), result);
        }else{
            result = getAttributes(op.getInputs().get(0), result);
        }

        return result;
    }


    // Assumes no joins in canonical plan
    private List<Operator> getOperators(Operator op, List<Operator> result){
        if(op instanceof Scan){
            result.add(op);
            return result;
        }

        if(op instanceof Product){
            result = getOperators(((Product) op).getLeft(), result);
            result = getOperators(((Product) op).getRight(), result);
            result.add(op);
        }else{
            result = getOperators(op.getInputs().get(0), result);
            result.add(op);
        }

        return result;
    }
}

