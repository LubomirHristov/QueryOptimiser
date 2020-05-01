package sjdb;

import org.w3c.dom.Attr;

import javax.smartcardio.ATR;
import java.util.ArrayList;
import java.util.List;

public class Optimiser {
    private Catalogue catalogue;

    public Optimiser(Catalogue catalogue){
        this.catalogue = catalogue;
    }

    public Operator optimise(Operator op){
        List<Operator> operators = getOperators(op, new ArrayList<>());

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

        // Move select in the form attr=val
        for(Scan scan: scanList){
            Operator optimisedScan = scan;

            for(Select select: selectList){
                if(select.getPredicate().equalsValue() && scan.getRelation().getAttributes().contains(select.getPredicate().getLeftAttribute())){
                    optimisedScan = new Select(optimisedScan, select.getPredicate());
                }
            }

            optimisedScanList.add(optimisedScan);
        }

        List<Operator> optimisedProductList = new ArrayList<>();

        // Reorder joins

        // Make all pairs for product 1

        List<List<Operator>> pairs = new ArrayList<>();

        for(int i = 0; i < optimisedScanList.size() - 1; i++){
            for(int j = i + 1; j < optimisedScanList.size(); j++){
                List<Operator> pair = new ArrayList<>();
                pair.add(optimisedScanList.get(i));
                pair.add(optimisedScanList.get(j));
                pairs.add(pair);
            }
        }

        // Create product 1

        for(int p = 0; p < productList.size(); p++){
            if(p == 0){
                Operator bestProduct = null;
                int bestOutputSize = -1;
                List<Operator> inputsUsed = new ArrayList<>();

                for(List<Operator> pair: pairs){
                    Operator product = new Product(pair.get(0), pair.get(1));
                    List<Attribute> productAttributes = getAttributes(product, new ArrayList<>());

                    // Match select in the form attr=attr with product to estimate cost
                    for(Select select: selectList){
                        if(!select.getPredicate().equalsValue() && productAttributes.contains(select.getPredicate().getLeftAttribute()) && productAttributes.contains(select.getPredicate().getRightAttribute())){
                            if(product instanceof Product){
                                product = new Join(((Product) product).getLeft(), ((Product) product).getRight(), select.getPredicate());
                            }else{
                                product = new Select(product, select.getPredicate());
                            }
                        }
                    }

                    Estimator estimator = new Estimator();
                    product.accept(estimator);

                    if(product.getOutput().getTupleCount() > bestOutputSize){
                        bestProduct = product;
                        bestOutputSize = product.getOutput().getTupleCount();
                        inputsUsed = pair;
                    }
                }

                bestProduct.setOutput(null);
                optimisedProductList.add(bestProduct);
                optimisedScanList.remove(inputsUsed.get(0));
                optimisedScanList.remove(inputsUsed.get(1));
            }else{
                // for every next product match previous product with the best relation

                Operator bestProduct = null;
                int bestOutputSize = -1;
                Operator inputUsed = null;


                for(int i = 1; i < productList.size(); i++){
                    for(Operator optimisedScan: optimisedScanList){
                        Operator product = new Product(optimisedProductList.get(optimisedProductList.size() - 1), optimisedScan);
                        inputUsed = optimisedScan;
                        List<Attribute> productAttributes = getAttributes(product, new ArrayList<>());

                        // Match select in the form attr=attr with product to estimate cost
                        for(Select select: selectList){
                            if(!select.getPredicate().equalsValue() && productAttributes.contains(select.getPredicate().getLeftAttribute()) && productAttributes.contains(select.getPredicate().getRightAttribute())){
                                if(product instanceof Product){
                                    product = new Join(((Product) product).getLeft(), ((Product) product).getRight(), select.getPredicate());
                                }else{
                                    product = new Select(product, select.getPredicate());
                                }
                            }
                        }

                        Estimator estimator = new Estimator();
                        product.accept(estimator);

                        if(product.getOutput().getTupleCount() > bestOutputSize){
                            bestProduct = product;
                            bestOutputSize = product.getOutput().getTupleCount();
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
        }else{
            result = getAttributes(op.getInputs().get(0), result);
        }

        return result;
    }


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




//if(op instanceof Scan){
//        return op;
//        }
//
//        List<Operator> operators = getOperators(op, new ArrayList<>());
//        List<Select> selectOperators = new ArrayList<>();
//
//        for(Operator operator: operators){
//        if(operator instanceof Select){
//        selectOperators.add((Select) operator);
//        }
//        }
//
//        Operator optimisedPlan = null;
//        List<Operator> children = new ArrayList<>();
//
//        for(int i = 0; i <= operators.size() - 1; i++){
//        Operator currentOperator = operators.get(i);
//
//        if(currentOperator instanceof Scan){
//        for(Select select: selectOperators){
//        if(select.getPredicate().equalsValue() && ((Scan)currentOperator).getRelation().getAttributes().contains(select.getPredicate().getLeftAttribute())){
//        currentOperator = new Select(currentOperator, select.getPredicate());
//        }
//        }
//
//        children.add(currentOperator);
//        }else if(currentOperator instanceof Product){
//        optimisedPlan = new Product(children.get(0), children.get(1));
//
//        for(Select select: selectOperators){
//        List<Attribute> currentOperatorAttributes = getAttributes(currentOperator, new ArrayList<>());
//
//        if(!select.getPredicate().equalsValue() && currentOperatorAttributes.contains(select.getPredicate().getLeftAttribute()) && currentOperatorAttributes.contains(select.getPredicate().getRightAttribute())){
//        optimisedPlan = new Select(optimisedPlan, select.getPredicate());
//        }
//        }
//
//        children.clear();
//        children.add(optimisedPlan);
//        }else if(currentOperator instanceof Select){
//        if(!shouldSkipSelect(optimisedPlan, (Select) currentOperator)){
//        if(optimisedPlan == null){
//        optimisedPlan = children.get(0);
//        }else{
//        optimisedPlan = new Select(optimisedPlan, ((Select) currentOperator).getPredicate());
//        }
//        }
//        }else if(currentOperator instanceof Project){
//        optimisedPlan = new Project(optimisedPlan, ((Project) currentOperator).getAttributes());
//        }
//        }
//
//        return optimisedPlan;


//    private boolean shouldSkipSelect(Operator optimisedPlan, Select selectToAdd){
//        if(optimisedPlan == null){
//            return false;
//        }
//
//        List<Operator> operators = getOperators(optimisedPlan, new ArrayList<>());
//        List<Select> selectOperators = new ArrayList<>();
//
//        for(Operator operator: operators){
//            if(operator instanceof Select){
//                selectOperators.add((Select) operator);
//            }
//        }
//
//        boolean shouldSkipSelect = false;
//
//        for(Select existingSelect: selectOperators){
//            if(selectToAdd.getPredicate().equals(existingSelect.getPredicate())){
//                shouldSkipSelect = true;
//            }
//        }
//
//        return shouldSkipSelect;
//    }
//
