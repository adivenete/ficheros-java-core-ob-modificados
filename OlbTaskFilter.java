package com.doceleguas.warehouse.advancedwarehouseoperations.olb.ad_process;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ObjectUtils;
import org.openbravo.base.util.ArgumentException;
import org.openbravo.dal.service.OBDal;
import org.openbravo.dal.service.OBQuery;
import org.openbravo.warehouse.advancedwarehouseoperations.OBAWOTask;

import com.doceleguas.warehouse.advancedwarehouseoperations.olb.data.TaskAssignConfig;

/**
 * Helper to filter tasks by {@link rule} ordered by priority(desc), Travel Seq(asc), bin(asc)
 * 
 * @see Rule
 * @author alain.perez@doceleguas.com
 */

public class OlbTaskFilter {

    private final StringBuilder       queryBuilder    = new StringBuilder();
    private final Map<String, Object> taskQueryParams = new HashMap<>(10);
    private TaskAssignConfig          rule;

    /**
     * Apply filters to tasks
     * 
     * @param ruleParam
     *            The rule that contains the filters to apply
     * @return List<OBAWOTask>
     */
    public List<OBAWOTask> filter(final TaskAssignConfig ruleParam) {

        this.rule = ruleParam;

        initializeFromAndWhereClause();
        filterByITT();
        filterByIRA();
        filterBySBG();
        filterByTaskType();
        filterByProduct();
        filterByProductCategory();
        filterByProductBrand();
        filterByPopCode();
        filterByTravelSequence();
        filterByUnassignedTask();
        filterByTaskStatusAndWarehouse();
        addOrderByClause();

        return getTaskList();
    }

    private void initializeFromAndWhereClause() {

        queryBuilder.append(" as task");
        queryBuilder.append(" inner join OBAWO_InventoryTransactionType itt on itt.id=task.inventoryTransactionType.id");
        queryBuilder.append("  inner join OBAWO_Internal_Routing r on r.id=task.internalRouting.id");
        queryBuilder.append("  left join task.expectedLocatorFrom bin_fr");
        queryBuilder.append("  left join task.expectedLocatorTo bin_to");
        queryBuilder.append("  left join r.internalRoutingAreaFrom iraF");
        queryBuilder.append("  left join r.internalRoutingAreaTo iraT");
        queryBuilder.append(" where 1=1");
    }

    private void filterByITT() {

        if (rule.getObawoInvTranType() != null) {
            queryBuilder.append(" and task.inventoryTransactionType.id = :itt");
            taskQueryParams.put("itt", rule.getObawoInvTranType().getId());
        }
    }

    private void filterByIRA() {

        if (rule.getObawoInternalRoutingArea() != null) {
            queryBuilder.append(" AND");
            queryBuilder.append(" CASE");
            queryBuilder.append("    WHEN");
            queryBuilder.append("       itt.obawoTravelsequenceFromto='FR'");
            queryBuilder.append("    THEN iraF.id");
            queryBuilder.append("    ELSE iraT.id");
            queryBuilder.append(" END");
            queryBuilder.append(" = :ira");
            taskQueryParams.put("ira", rule.getObawoInternalRoutingArea().getId());
        }
    }

    private void filterBySBG() {

        if (rule.getObawoLocatorGroup() != null) {
            queryBuilder.append(" AND");
            queryBuilder.append(" CASE");
            queryBuilder.append("    WHEN");
            queryBuilder.append("       itt.obawoTravelsequenceFromto='FR'");
            queryBuilder.append("    THEN bin_fr.oBAWOLocatorGroup.id");
            queryBuilder.append("    ELSE bin_to.oBAWOLocatorGroup.id");
            queryBuilder.append(" END");
            queryBuilder.append(" = :sbg");
            taskQueryParams.put("sbg", rule.getObawoLocatorGroup().getId());
        }
    }

    private void filterByTaskType() {

        if (rule.getObawoTaskType() != null) {
            String tt = rule.getObawoTaskType().getId();
            queryBuilder.append(" and task.taskType.id= :tt");
            taskQueryParams.put("tt", tt);
        }
    }

    private void filterByProduct() {

        if (rule.getProduct() != null) {
            String prod = rule.getProduct().getId();
            queryBuilder.append(" and task.product.id= :product");
            taskQueryParams.put("product", prod);
        }
    }

    private void filterByProductBrand() {

        if (rule.getBrand() != null) {
            String brand = rule.getBrand().getId();
            queryBuilder.append(" and task.product.brand.id= :brand");
            taskQueryParams.put("brand", brand);
        }
    }

    private void filterByProductCategory() {

        if (rule.getProductCategory() != null) {
            String cat = rule.getProductCategory().getId();
            queryBuilder.append(" and task.product.productCategory.id= :category");
            taskQueryParams.put("category", cat);
        }
    }

    private void filterByPopCode() {

        if (rule.getPopularityCode() != null) {
            String popcode = parsePopCodeParam(rule.getPopularityCode());
            queryBuilder.append(String.format(" and (bin_fr.oBAWOPopularityCode %s or bin_to.oBAWOPopularityCode %s)", popcode, popcode));
        }
    }

    private void filterByTravelSequence() {

        if (rule.getTravelsequence() != null && "%".equals(rule.getTravelsequence()) == false) {
            String travel_seq = rule.getTravelsequence();
            queryBuilder.append(" and task.obawoTravelSequence LIKE :sequence ");
            taskQueryParams.put("sequence", travel_seq);
        }
    }

    private void filterByTaskStatusAndWarehouse() {

        queryBuilder.append(" and task.status='AV'");
        queryBuilder.append(" and task.warehouse.id = :warehouse");
        taskQueryParams.put("warehouse", rule.getWarehouse().getId());
    }

    private void filterByUnassignedTask() {

        queryBuilder.append(" and task.userContact is NULL");
    }

    private String parsePopCodeParam(String _popCode) throws ArgumentException {

        String popCode = _popCode;
        String pattern = "([><]?\\s*\\d+)";
        if (!_popCode.matches(pattern)) {
            throw new ArgumentException("Invalid popularity code");
        }
        pattern = "^[1-9]+[0-9]*";
        if (popCode.matches(pattern)) return "=" + popCode;
        return popCode;
    }

    /**
     * Order by:
     * <p>
     * - task priority desc
     * <p>
     * - travel sequence asc
     * <p>
     * - If Travel Sequence Bin on ITT is from-bin the binfrom is taken else binto is taken. If Order
     * by Bin field is defined in the rule the bins are ordered by this field, in other case the bins
     * are ordered asc
     */
    private void addOrderByClause() {

        final String orderByBin = (String) ObjectUtils.defaultIfNull(this.rule.getOrderByBin(), "asc");
        queryBuilder.append(" order by task.priority desc, task.creationDate asc, task.obawoTravelSequence asc");
        queryBuilder.append(", CASE");
        queryBuilder.append("    WHEN");
        queryBuilder.append("       itt.obawoTravelsequenceFromto='FR'");
        queryBuilder.append("    THEN bin_fr.searchKey");
        queryBuilder.append("    ELSE bin_to.searchKey");
        queryBuilder.append("  END ");
        queryBuilder.append(orderByBin);
    }

    private List<OBAWOTask> getTaskList() {

        final OBQuery<OBAWOTask> taskQuery = OBDal.getInstance().createQuery(OBAWOTask.class, queryBuilder.toString());

        taskQuery.setNamedParameters(taskQueryParams);
        taskQuery.setFilterOnReadableClients(false);
        taskQuery.setFilterOnReadableOrganization(false);

        return taskQuery.list();
    }
}
