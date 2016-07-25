package agent;

import java.io.*;
import java.util.*;

import org.apache.log4j.Logger;

// preload some useful ID-to-website mappings, both from Cassandra and the config files

public class Preload {
    protected static Logger logger = Logger.getLogger(Preload.class.getName());

    private static Preload instance = null;

    private Map<String,String> storeToWebsite;
    private Map<String,String> productToWebsite;
    private Set<String> voucherTemplateToWebsiteExists;

	public Preload() throws Exception {

        // preload store->website mapping
        storeToWebsite = new HashMap<String,String>();
        CassandraReader reader = new CassandraReader("Store", new String[]{"websiteId"}, 1000);
        while (true) {
            RowBlock block = reader.readSequential();
            if (block.rows.isEmpty()) {
                break;
            }
            for (Row row : block.rows) {
                String storeId = row.key;
                String websiteId = row.columns.get("websiteId");
                if (websiteId != null) {
                    storeToWebsite.put(storeId, websiteId);
                }
            }
        }

        // preload product->website mapping
        productToWebsite = new HashMap<String,String>();
        reader = new CassandraReader("Product", new String[]{"websiteId"}, 1000);
        while (true) {
            RowBlock block = reader.readSequential();
            if (block.rows.isEmpty()) {
                break;
            }
            for (Row row : block.rows) {
                String productId = row.key;
                String websiteId = row.columns.get("websiteId");
                if (websiteId != null) {
                    productToWebsite.put(productId, websiteId);
                }
            }
        }

        // preload voucher template->website mapping
        voucherTemplateToWebsiteExists = new HashSet<String>();
        reader = new CassandraReader("VoucherTemplate", new String[]{"websiteIds"}, 1000);
        while (true) {
            RowBlock block = reader.readSequential();
            if (block.rows.isEmpty()) {
                break;
            }
            for (Row row : block.rows) {
                String voucherTemplateId = row.key;
                String websiteIds = row.columns.get("websiteIds");
                if (websiteIds != null) {
                    String[] arrayIds = websiteIds.split("(,)");
                    for (String id : arrayIds ) {
                        voucherTemplateToWebsiteExists.add(voucherTemplateId + ":" + id);
                    }
                }
            }
        }

        logger.info(
            storeToWebsite.size() + " stores, " +
            productToWebsite.size() + " products, " +
            voucherTemplateToWebsiteExists.size()  + " voucher templates"
        );
	}

    public static void init() throws Exception {
        Preload.instance = new Preload();
    }

    public static String websiteForStoreID(String storeID) {
        String r;
        synchronized (Preload.instance) {
            r = Preload.instance.storeToWebsite.get(storeID);
        }
        return r;
    }

    public static String websiteForProductID(String productID) {
        String r;
        synchronized (Preload.instance) {
            r = Preload.instance.productToWebsite.get(productID);
        }
        return r;
    }

    public static boolean websiteForVoucherTemplateIDExists(String voucherTemplateID, String websiteID) {
        boolean r;
        synchronized (Preload.instance) {
            r = Preload.instance.voucherTemplateToWebsiteExists.contains(voucherTemplateID + ":" + websiteID);
        }
        return r;
    }

}
