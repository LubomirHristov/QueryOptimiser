/**
 * 
 */
package sjdb;
import java.io.*;

/**
 * @author nmg
 *
 */
public class SJDB {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// read serialised catalogue from file and parse
		String catFile = args[0];
		Catalogue cat = new Catalogue();
		CatalogueParser catParser = new CatalogueParser(catFile, cat);
		catParser.parse();

		// read stdin, parse, and build canonical query plan
		QueryParser queryParser = new QueryParser(cat, new InputStreamReader(System.in));
		Operator plan = queryParser.parse();

		// create estimator visitor and apply it to canonical plan
		Inspector inspector = new Inspector();
		Estimator est = new Estimator();
		plan.accept(est);
		plan.accept(inspector);

		System.out.println("*******************************************");

		// create optimised plan
		Optimiser opt = new Optimiser(cat);
		Operator optPlan = opt.optimise(plan);
		Inspector inspector2 = new Inspector();
		Estimator est2 = new Estimator();
		optPlan.accept(est2);
		optPlan.accept(inspector2);
	}

}
