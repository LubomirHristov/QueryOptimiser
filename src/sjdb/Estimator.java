package sjdb;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

public class Estimator implements PlanVisitor {


	public Estimator() {
		// empty constructor
	}

	/* 
	 * Create output relation on Scan operator
	 *
	 * Example implementation of visit method for Scan operators.
	 */
	public void visit(Scan op) {
		Relation input = op.getRelation();
		Relation output = new Relation(input.getTupleCount());
		
		Iterator<Attribute> iter = input.getAttributes().iterator();
		while (iter.hasNext()) {
			output.addAttribute(new Attribute(iter.next()));
		}
		
		op.setOutput(output);
	}

	public void visit(Project op) {
		Relation input = op.getInput().getOutput();

		// Output size stays the same
	    Relation output = new Relation(input.getTupleCount());

	    Iterator<Attribute> iter = input.getAttributes().iterator();
	    while (iter.hasNext()) {
	    	Attribute attr = iter.next();

	    	// Check if each attribute from the input relation is in the specified attributes for projection
	    	if(op.getAttributes().contains(attr)){
	    		output.addAttribute(new Attribute(attr));
			}
		}

	    op.setOutput(output);
	}
	
	public void visit(Select op) {
		Relation input = op.getInput().getOutput();
		boolean predicateEqualsValue = op.getPredicate().equalsValue();
		Attribute leftAttribute = op.getInput().getOutput().getAttribute(op.getPredicate().getLeftAttribute());


		if(predicateEqualsValue){
			// Output size = T(R)/V(R, A)
			Relation output = new Relation((int) Math.ceil((double)input.getTupleCount()/leftAttribute.getValueCount()));

			Iterator<Attribute> iter = input.getAttributes().iterator();
			while (iter.hasNext()) {
				Attribute attr = iter.next();

				if(attr.equals(leftAttribute)){
					output.addAttribute(new Attribute(attr.getName(), 1));
				}else{
					output.addAttribute(new Attribute(attr));
				}
			}

			op.setOutput(output);
		}else{
			Attribute rightAttribute = op.getInput().getOutput().getAttribute(op.getPredicate().getRightAttribute());

			// Get the value counts of the left and right attributes that are being selected
			int vcount1 = leftAttribute.getValueCount();
			int vcount2 = rightAttribute.getValueCount();

			// Get the max value count from the selected attributes
			int maxValueCount = this.getMaxValueCount(vcount1, vcount2);

			// Get the min value count from the selected attributes
			int minValueCount = this.getMinValueCount(vcount1, vcount2);

			// Output size = T(R)/max(V(R, A),V(R, B))
			Relation output = new Relation((int) Math.ceil((double)input.getTupleCount()/maxValueCount));

			Iterator<Attribute> iter = input.getAttributes().iterator();
			while (iter.hasNext()) {
				Attribute attr = iter.next();

				if(attr.equals(leftAttribute) || attr.equals(rightAttribute)){
					output.addAttribute(new Attribute(attr.getName(), minValueCount));
				}else{
					output.addAttribute(new Attribute(attr));
				}
			}

			op.setOutput(output);
		}
	}
	
	public void visit(Product op) {
		// Output size = T(L) * T(R)
		Relation output = new Relation((int) Math.ceil((double)op.getLeft().getOutput().getTupleCount() * op.getRight().getOutput().getTupleCount()));

		// Get all attributes from the 2 Relations
		List<Attribute> leftAttributes = op.getLeft().getOutput().getAttributes();
		List<Attribute> rightAttributes = op.getRight().getOutput().getAttributes();

		Iterator<Attribute> iter1 = leftAttributes.iterator();
		Iterator<Attribute> iter2 = rightAttributes.iterator();

		// Add each attribute from the 2 Relations
		while (iter1.hasNext()) {
			output.addAttribute(new Attribute(iter1.next()));
		}

		while (iter2.hasNext()) {
			output.addAttribute(new Attribute(iter2.next()));
		}

		op.setOutput(output);
	}
	
	public void visit(Join op) {
		// Get the value counts of the left and right attributes that are being joined
		int vcount1 = op.getLeft().getOutput().getAttribute(op.getPredicate().getLeftAttribute()).getValueCount();
		int vcount2 = op.getRight().getOutput().getAttribute(op.getPredicate().getRightAttribute()).getValueCount();

		int maxValueCount = this.getMaxValueCount(vcount1, vcount2);

		// Output size = (T(L) * T(R))/max(V(R, A),V(R, B))
		Relation output = new Relation(((int) Math.ceil((double)(op.getLeft().getOutput().getTupleCount() * op.getRight().getOutput().getTupleCount())/maxValueCount)));

		List<Attribute> leftAttributes = op.getLeft().getOutput().getAttributes();
		List<Attribute> rightAttributes = op.getRight().getOutput().getAttributes();

		Iterator<Attribute> iter1 = leftAttributes.iterator();
		Iterator<Attribute> iter2 = rightAttributes.iterator();

		// Add all attributes from the first Relation
		while (iter1.hasNext()) {
			output.addAttribute(new Attribute(iter1.next()));
		}

		// Add only non-duplicate attributes from the second Relation
		while (iter2.hasNext()) {
			Attribute attr = iter2.next();
			if(!leftAttributes.contains(attr)){
				output.addAttribute(new Attribute(attr));
			}
		}

		op.setOutput(output);
	}

	private int getMaxValueCount(int vcount1, int vcount2){
		return vcount1 > vcount2 ? vcount1 : vcount2;
	}

	private int getMinValueCount(int vcount1, int vcount2){
		return vcount1 > vcount2 ? vcount2 : vcount1;
	}
}
