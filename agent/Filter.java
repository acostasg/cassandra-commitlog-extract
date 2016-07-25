package agent;

import java.util.*;

import org.apache.log4j.Logger;

public class Filter {
	protected static Logger logger = Logger.getLogger(Filter.class.getName());

	private Set<String> allowedWebsites;
	private Set<String> notFilterAllowed;

	public Filter(Map<String,Object> conf) throws Exception {
		
		if (conf.containsKey("allowedWebsites")) {
			allowedWebsites = new HashSet<String>();
			@SuppressWarnings("unchecked")
			List<String> allowedWebsitesL = (List<String>)conf.get("allowedWebsites");
			for (String website : allowedWebsitesL) {
				allowedWebsites.add(website);
			}
		}
		
		notFilterAllowed = new HashSet<String>();
		if (conf.containsKey("notFilterAllowed")) {
			@SuppressWarnings("unchecked")
			List<String> notFilterAllowedL = (List<String>)conf.get("notFilterAllowed");
			for (String discartedTable : notFilterAllowedL) {
				notFilterAllowed.add(discartedTable);
			}
		}
		
	}

	private boolean isValidWebsite(Row row) {

		boolean hasField = false;
	
		// direct case
		String websiteId = row.columns.get("websiteId");
		if (websiteId != null) {
			hasField = true;
			if (allowedWebsites.contains(websiteId)) {
				//logger.info("ACCEPTED " + row.key + ", reason: websiteId");
				return true;
			}
		}

		// lookup by store ID
		String storeId = row.columns.get("storeId");
		if (storeId != null) {
			hasField = true;
			websiteId = Preload.websiteForStoreID(storeId);
			if (websiteId != null) {
				if (allowedWebsites.contains(websiteId))
					//logger.info("ACCEPTED " + row.key + ", reason: storeId");
					return true;
			}
		}

		// lookup by product ID
		String productId = row.columns.get("productId");
		if (productId != null) {
			hasField = true;
			websiteId = Preload.websiteForProductID(productId);
			if (websiteId != null) {
				if (allowedWebsites.contains(websiteId)) {
					//logger.info("ACCEPTED " + row.key + ", reason: productId");
					return true;
				}
			}
		}

		// lookup by "type" (special case for vouchers)
		String type = row.columns.get("type");
		if (type != null) {
			hasField = true;
			for (String websiteIdA : allowedWebsites) {
				if (Preload.websiteForVoucherTemplateIDExists(type, websiteIdA)) {
					//logger.info("ACCEPTED " + row.key + ", reason: type");
					return true;
				}
			}
		}

		//if (hasField)
		//	logger.info("REJECTED " + row.key + " reason: some checked fields were present but none matched");

		return !hasField;
	}
	
	public boolean isValid(Row row) {
		return isValidWebsite(row);
	}
	
    public boolean tableNotFilter( String table ){
    	if (notFilterAllowed.isEmpty()) return false;
		for (String discartedTable : notFilterAllowed) {
			if (discartedTable.toString().toLowerCase().trim().equals(table.toString().toLowerCase().trim())){
				return true;
			}			
		}
    	return false;
    }

}
