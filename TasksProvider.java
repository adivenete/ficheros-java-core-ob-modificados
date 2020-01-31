/*
 ************************************************************************************
 * Copyright (C) 2016-2018 Openbravo S.L.U.
 * Licensed under the Openbravo Commercial License version 1.0
 * You may obtain a copy of the License at http://www.openbravo.com/legal/obcl.html
 * or in the legal folder of this module distribution.
 ************************************************************************************
 */

package org.openbravo.warehouse.advancedwarehouseoperations.mobile.datasources;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;

import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.hibernate.ScrollMode;
import org.hibernate.ScrollableResults;
import org.hibernate.Session;
import org.hibernate.query.Query;
import org.openbravo.client.kernel.ComponentProvider.Qualifier;
import org.openbravo.dal.core.OBContext;
import org.openbravo.dal.service.OBDal;
import org.openbravo.erpCommon.businessUtility.Preferences;
import org.openbravo.erpCommon.utility.PropertyException;
import org.openbravo.mobile.core.model.HQLPropertyList;
import org.openbravo.mobile.core.model.ModelExtension;
import org.openbravo.mobile.core.model.ModelExtensionUtils;
import org.openbravo.mobile.core.process.Scroll;
import org.openbravo.warehouse.advancedwarehouseoperations.mobile.utils.OBAWOProcessHQLQuery;

public class TasksProvider extends OBAWOProcessHQLQuery {
	public static final String taskProviderPropertyExtension = "OBAWO_TaskProviderExtension";
	public static final Logger log = Logger.getLogger(TasksProvider.class);
	private boolean hasParentsIds = false;
	@Inject
	@Any
	@Qualifier(taskProviderPropertyExtension)
	private Instance<ModelExtension> extensions;

	/*
	 * DEVELOPERS important note: getParameterValues function is executed just one
	 * time before retrieve queries. Here in this function, the query to retrieve
	 * tasks is pre executed. With this execution we will be able to know if there
	 * are parent task (isHeader==true) in the selection.
	 * 
	 * If there are parent tasks, then we will execute 2 different queries 1-> To
	 * retrieve tasks and headers (parent tasks) 2-> To retrieve childs of all of
	 * the parent tasks which were selected in the first query
	 * 
	 * In case that pre execution of the queries results on 0 parent queries, 2nd
	 * query will not be executed.
	 */

	@Override
	protected Map<String, Object> getParameterValues(JSONObject jsonsent) throws JSONException {
		Map<String, Object> paramValues = new HashMap<>();
		final String userId = OBContext.getOBContext().getUser().getId();
		String warehouseId = "";
		JSONObject params = jsonsent.getJSONObject("parameters");
		if (!params.isNull("warehouse")) {
			warehouseId = params.getJSONObject("warehouse").getString("value");
		} else {
			throw new JSONException("Warehouse Id is required to retrieve tasks");
		}

		String limit = "30";
		// read limit preference
		try {
			limit = Preferences.getPreferenceValue("OBAWO_LimitOfTaskForMobile", true, null, null, null, null,
					(String) null);
		} catch (final PropertyException e) {
			// nothing to do
		}

		HQLPropertyList regularTasksHQLProperties = ModelExtensionUtils.getPropertyExtensions(extensions, false);

		this.hasParentsIds = false;
		// Execute generic query selecting tasks and headers using limit
		// Based on results of this execution, data to send to the client side is
		// selected
		String allHqlQuery = this.getStringQuery(false);
		final Session session = OBDal.getInstance().getSession();
		final Query<Object[]> query = session.createQuery(allHqlQuery, Object[].class);
		query.setMaxResults(Integer.parseInt(limit));
		query.setParameter("userId", userId);
		query.setParameter("warehouseId", warehouseId);
		query.setParameter("availableStatus", "AV");
		ScrollableResults listdata = query.scroll(ScrollMode.FORWARD_ONLY);
		Scroll mobileCoreScroll = Scroll.create(listdata);
		// Iterate results. Pick those results which are headers
		List<String> arrSelectedReceptionLists = new ArrayList<>();
		while (mobileCoreScroll.next()) {
			if ((Boolean) listdata.get(regularTasksHQLProperties.getHqlPropertyIndex("isHeader"))) {
				arrSelectedReceptionLists
						.add((String) listdata.get(regularTasksHQLProperties.getHqlPropertyIndex("id")));
			}
		}

		if (arrSelectedReceptionLists.size() > 0) {
			this.hasParentsIds = true;
			paramValues.put("parentDocIds", arrSelectedReceptionLists);
		} else {
			// It means that query for grouped tasks will not be executed because there are
			// not child
			// tasks to be retrieved. If no parent tasks -> No child tasks can exist
			// Parameter parentDocIds is not set because it will not appear in the query
			this.hasParentsIds = false;
		}

		paramValues.put("userId", userId);
		paramValues.put("warehouseId", warehouseId);
		paramValues.put("availableStatus", "AV");
		return paramValues;
	}

	@Override
	protected List<String> getQuery(JSONObject jsonsent) throws JSONException {
		List<String> tasksQueries = new ArrayList<String>();
		// get tasks and headers (parents)
		tasksQueries.add(this.getStringQuery(false));

		if (this.hasParentsIds) {
			// get tasks which are childs of the parent tasks selected in previous query
			tasksQueries.add(this.getStringQuery(true));
		}

		return tasksQueries;
	}

	private String getStringQuery(Boolean isGroupQuery) throws JSONException {
		HQLPropertyList regularTasksHQLProperties = ModelExtensionUtils.getPropertyExtensions(extensions, isGroupQuery);

		// regular tasks
		StringBuilder hqlStringBuilder = new StringBuilder("select DISTINCT ");
		hqlStringBuilder.append(regularTasksHQLProperties.getHqlSelect());
		hqlStringBuilder.append("from OBAWO_Task t left join t.receptionList as rl left JOIN t.batchOfTasks as bot ");
		hqlStringBuilder.append(" left join t.warehousePickingList as pl ");
		hqlStringBuilder.append(" left join pl.referencedInventory as plRefInv ");
		hqlStringBuilder.append(" left join t.expectedLocatorTo as expectedLocTo ");
		hqlStringBuilder.append(" left join t.expectedLocatorFrom as expectedLocFrom ");
		hqlStringBuilder.append(" left join t.confirmedLocatorTo as confirmedLocTo ");
		hqlStringBuilder.append(" left join t.confirmedLocatorFrom as confirmedLocFrom ");
		hqlStringBuilder.append(" left join t.storageDetail as stgDetail ");
		hqlStringBuilder.append(" left join stgDetail.referencedInventory as currentRefInv ");
		hqlStringBuilder.append(" left join t.expectedAttribute as expectedAtt ");
		hqlStringBuilder.append(" left join t.internalRouting as intRouting ");
		hqlStringBuilder.append(" inner join t.taskType as tType ");
		hqlStringBuilder.append(" inner join tType.baseTaskType as baseTType ");
		hqlStringBuilder.append(" left join t.expectedMRefinventory as expectedRefInv ");
		hqlStringBuilder.append(" left join t.confirmedMRefinventory as confirmedRefInv ");
		hqlStringBuilder.append(" left join t.alternativeUOM as altUom ");
		hqlStringBuilder.append(" left join t.internalRouting.internalRoutingAreaFrom as externalRoutingAreaFrom ");
		hqlStringBuilder.append(" left join t.internalRouting.internalRoutingAreaTo as externalRoutingAreaTo ");
		hqlStringBuilder.append(" left join t.product as product ");
		hqlStringBuilder.append(" left join t.product.uOM as uom ");
		hqlStringBuilder.append(" left join t.product.attributeSet as attset ");
		hqlStringBuilder.append(" where t.userContact.id = :userId ");

		if (isGroupQuery) {
			// Pick just child tasks of parent tasks selected in the first execution of the
			// query.
			// selected headers excluding those which were not results in the previous
			// execution of the
			// query
			hqlStringBuilder.append(
					" AND (t.receptionList is not null OR (t.warehousePickingList is not null AND t.behaveAsGroupWithReferencedInventory = 'Y')) ");
			hqlStringBuilder.append(
					" AND (t.receptionList.id in (:parentDocIds) OR t.warehousePickingList.id in (:parentDocIds))");
		} else {
			if (this.hasParentsIds) {
				// ParentDocIds parameter only will be set if there are childs.
				// If there are not childs these additional filters does not make sense
				// so they are skipped
				hqlStringBuilder.append(" AND ");
				hqlStringBuilder.append("  (");
				hqlStringBuilder.append("   t.receptionList is null OR ");
				hqlStringBuilder.append("   t.receptionList.id in (:parentDocIds)");
				hqlStringBuilder.append("  )");
			}
		}

		hqlStringBuilder.append(" AND not exists (");
		hqlStringBuilder.append("  select 1 from OBAWO_Error_Task terr");
		hqlStringBuilder.append(
				"  where terr.task.id = t.id and terr.oBAWOError.taskstatus = 'N' and terr.oBAWOError.blocks = 'Y'");
		hqlStringBuilder.append(" )");
		hqlStringBuilder.append(" AND t.warehouse.id = :warehouseId");
		hqlStringBuilder.append(" AND t.status = :availableStatus ");
		hqlStringBuilder.append("order by creationDate asc");
		return hqlStringBuilder.toString();
	}

	@Override
	protected boolean bypassPreferenceCheck() {
		return true;
	}
}
