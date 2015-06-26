import java.util.Comparator;
import java.util.Map;

class ValueComparator implements Comparator<Object> {
 
	Map<String, Float> map;
 
	public ValueComparator(Map<String, Float> map) {
		this.map = map;
	}
 
	public int compare(Object keyA, Object keyB) {
		
		Float valueA= (Float) map.get(keyA);
		Float valueB= (Float) map.get(keyB);
		
		int compare=valueB.compareTo(valueA);
		
		if(compare==0)
			return 1;		
		return compare;
	}
}