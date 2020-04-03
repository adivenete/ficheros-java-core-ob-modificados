/*
 ************************************************************************************
 * Copyright (C) 2016-2018 Openbravo S.L.U.
 * Licensed under the Openbravo Commercial License version 1.0
 * You may obtain a copy of the License at http://www.openbravo.com/legal/obcl.html
 * or in the legal folder of this module distribution.
 ************************************************************************************
 */

package org.openbravo.warehouse.advancedwarehouseoperations.eventobserver;

import java.math.BigDecimal;

import javax.enterprise.event.Observes;

import org.apache.commons.lang.StringUtils;
import org.hibernate.Session;
import org.hibernate.criterion.Restrictions;
import org.hibernate.engine.internal.Cascade;
import org.hibernate.query.Query;
import org.openbravo.base.exception.OBException;
import org.openbravo.base.model.Entity;
import org.openbravo.base.model.ModelProvider;
import org.openbravo.base.model.Property;
import org.openbravo.base.structure.BaseOBObject;
import org.openbravo.client.kernel.event.EntityDeleteEvent;
import org.openbravo.client.kernel.event.EntityNewEvent;
import org.openbravo.client.kernel.event.EntityPersistenceEvent;
import org.openbravo.client.kernel.event.EntityPersistenceEventObserver;
import org.openbravo.client.kernel.event.EntityUpdateEvent;
import org.openbravo.dal.core.OBContext;
import org.openbravo.dal.service.OBCriteria;
import org.openbravo.dal.service.OBDal;
import org.openbravo.erpCommon.utility.OBMessageUtils;
import org.openbravo.model.common.enterprise.Locator;
import org.openbravo.model.common.order.Order;
import org.openbravo.model.common.order.OrderLine;
import org.openbravo.model.common.plm.AttributeSetInstance;
import org.openbravo.model.materialmgmt.onhandquantity.ReservationStock;
import org.openbravo.model.materialmgmt.onhandquantity.StorageDetail;
import org.openbravo.warehouse.advancedwarehouseoperations.OBAWOTask;
import org.openbravo.warehouse.advancedwarehouseoperations.centralbroker.CentralBrokerUtil;
import org.openbravo.warehouse.advancedwarehouseoperations.ittalgorithm.IssueSalesOrder_ITT;
import org.openbravo.warehouse.advancedwarehouseoperations.ittalgorithm.PickingSalesOrder_ITT;
import org.openbravo.warehouse.advancedwarehouseoperations.ittalgorithm.implementation.PickingSalesOrder_ITTAlgorithm;
import org.openbravo.warehouse.advancedwarehouseoperations.jsondataserviceextraactions.JsonDataServiceExtraActionsTaskDeletion;
import org.openbravo.warehouse.advancedwarehouseoperations.occupancy.OccupancyUtils;
import org.openbravo.warehouse.advancedwarehouseoperations.occupancy.OccupancyUtils.TaskEventChangingPendingOccupancyBin;
import org.openbravo.warehouse.advancedwarehouseoperations.utils.AWOReferencedInventoryUtils;
import org.openbravo.warehouse.advancedwarehouseoperations.utils.AWOReservationDeleteManager;
import org.openbravo.warehouse.advancedwarehouseoperations.utils.OBAWOException;
import org.openbravo.warehouse.advancedwarehouseoperations.utils.OBAWO_Constants;
import org.openbravo.warehouse.advancedwarehouseoperations.utils.TaskErrorsUtils;

import es.opentix.desguace.ventas.GeneracionOperacionesPedidos;

/**
 * EventObserver on Task entity in charge of different actions. <br/>
 * When inserting:
 * <ul>
 * <li>Checks the task protection is not broken (see
 * {@link #checkTaskProtection(OBAWOTask)}).</li>
 * </ul>
 * <br/>
 * 
 * When updating:
 * <ul>
 * <li>Avoids to update the confirmed fields of a confirmed task (see
 * {@link #checkIsConfirmedTask(EntityUpdateEvent)}).</li>
 * <li>Only for not confirmed tasks behaving as group with RI and when the
 * confirmed locator to or the user assigned are changed, updates the confirmed
 * locator to and/or the user assigned for any other task in the group</li>
 * </ul>
 * <br/>
 * 
 * When deleting:
 * <ul>
 * <li>Avoids to delete a confirmed task or a task which has an assigned
 * user.</li>
 * 
 * <li>Deletes associate reservation when deleting a not confirmed yet task (see
 * {@link AWOReservationDeleteManager#deleteReservation(OBAWOTask, BigDecimal)}.</li>
 * </ul>
 * <br/>
 * 
 * Besides it always updates both the Sales Order Line pending picking quantity
 * and the Sales Order Header Pending Picking Lines (see
 * {@link #updateSalesOrderLinePendingPickingQty(OBAWOTask, EntityPersistenceEvent)}).
 * 
 */
public class TaskEventObserver extends EntityPersistenceEventObserver {
	private static final String CASCADE_CLASSNAME = Cascade.class.getName();
	private static final String JSONDATASERVICEEXTRAACTIONSTASKDELETION_CLASSNAME = JsonDataServiceExtraActionsTaskDeletion.class
			.getName();

	private static final String GENERACIONOPERACIONESPEDIDOS_CLASSNAME = GeneracionOperacionesPedidos.class.getName();

	private static Entity[] entities = { ModelProvider.getInstance().getEntity(OBAWOTask.ENTITY_NAME) };

	@Override
	protected Entity[] getObservedEntities() {
		return entities;
	}

	/**
	 * Checks the task protection is not broken (see
	 * {@link #checkTaskProtection(OBAWOTask)}).
	 * 
	 * Updates Sales Order Line pending picking quantity and Sales Order Header
	 * Pending Picking Lines (see
	 * {@link #updateSalesOrderLinePendingPickingQty(OBAWOTask, EntityPersistenceEvent)}).
	 * 
	 * Updates the bins' pending occupancy
	 */
	public void onSave(@Observes EntityNewEvent event) {
		if (!isValidEvent(event)) {
			return;
		}

		final OBAWOTask task = (OBAWOTask) event.getTargetInstance();
		// FIXME this check can have a big impact in performance. To be removed when the
		// system is
		// stable enough.
		// This is required right now to ensure reservations don't steal storage
		// details. We need a hook
		// in reservation data source to avoid showing storage details in tasks
		checkTaskProtection(task);

		updateSalesOrderLinePendingPickingQty(task, event);
		updateBinsPendingOccupancyAtTaskCreation(task);
	}

	/**
	 * Avoids to update the confirmed fields of a confirmed task (see
	 * {@link #checkIsConfirmedTask(EntityUpdateEvent)}).
	 * 
	 * Updates Sales Order Line pending picking quantity and Sales Order Header
	 * Pending Picking Lines (see
	 * {@link #updateSalesOrderLinePendingPickingQty(OBAWOTask, EntityPersistenceEvent)}).
	 * 
	 * Only for not confirmed tasks behaving as group with RI and when the confirmed
	 * locator to or the user assigned are changed, updates the confirmed locator to
	 * and/or the user assigned for any other task in the group.
	 * 
	 */
	public void onUpdate(@Observes EntityUpdateEvent event) {
		if (!isValidEvent(event)) {
			return;
		}

		checkIsConfirmedTask(event);

		final OBAWOTask task = (OBAWOTask) event.getTargetInstance();
		updateSalesOrderLinePendingPickingQty(task, event);
		// Only in case the confirmed locator to has been changed by the user
		propagateConfirmedLocatorToOrUserAssignedChangeToRelatedBehaveAsGroupWithRITasks(task, event);
	}

	/**
	 * Avoids to delete a confirmed task or a task which has an assigned user. It
	 * also avoids to individually delete a behave as group with RI task
	 * 
	 * Deletes associate reservation when deleting a not confirmed yet task (see
	 * {@link #deleteReservation(OBAWOTask)}).
	 * 
	 * Updates Sales Order Line pending picking quantity and Sales Order Header
	 * Pending Picking Lines (see
	 * {@link #updateSalesOrderLinePendingPickingQty(OBAWOTask, EntityPersistenceEvent)}).
	 * 
	 * Updates the bins' pending occupancy
	 */
	public void onDelete(@Observes EntityDeleteEvent event) {
		if (!isValidEvent(event)) {
			return;
		}
		final OBAWOTask task = (OBAWOTask) event.getTargetInstance();
		if (task.getStatus().equals(OBAWO_Constants.STATUS_CONFIRMED)) {
			throw new OBException(OBMessageUtils.messageBD("OBAWO_DeletingProcessedTask"));
		} else if (task.getUserContact() != null) {
			throw new OBException(OBMessageUtils.messageBD("OBAWO_DeletingAssignedTask"));
		}

		throwExceptionIfIndividuallyDeletingABehaveAsGroupWithRITask(task);
		AWOReservationDeleteManager.deleteReservation(task, task.getExpectedQuantity());
		updateSalesOrderLinePendingPickingQty(task, event);
		updateBinsPendingOccupancyAtIndividualTaskDeletetion(task);
		TaskErrorsUtils.deleteErrorTaskEntries(task);
	}

	/**
	 * Checks there is enough stock available (i.e. not in previous not-confirmed
	 * tasks) before saving this task. The check is only done when the task is
	 * linked to a storage detail, it hasn't been created as a delta task and the
	 * storage bin doesn't allow overissue because in these case the check is not
	 * needed
	 */
	private void checkTaskProtection(OBAWOTask task) {
		try {
			OBContext.setAdminMode(true);
			final StorageDetail storageDetail = task.getStorageDetail();
			if (storageDetail != null && task.getDeltafromOBAWOTask() == null) {
				boolean isOverIsssue = storageDetail.getStorageBin().getInventoryStatus().isOverissue();
				if (!isOverIsssue) {
					final StringBuilder hql = new StringBuilder();
					hql.append("select coalesce(sum(t.expectedQuantity), 0) ");
					hql.append("from OBAWO_Task t ");
					hql.append("where t.status in (:notConfirmedStatuses) ");
					hql.append("and t.storageDetail.id = :storageDetailId ");

					final Session session = OBDal.getInstance().getSession();
					final Query<BigDecimal> query = session.createQuery(hql.toString(), BigDecimal.class);
					query.setParameterList("notConfirmedStatuses", OBAWO_Constants.STATUSES_NOT_CONFIRMED);
					query.setParameter("storageDetailId", storageDetail.getId());

					final BigDecimal currentTasksPendingQty = query.uniqueResult();
					final BigDecimal thisTaskPendingQty = task.getExpectedQuantity();
					final BigDecimal totalTasksPendingQty = currentTasksPendingQty.add(thisTaskPendingQty);
					final BigDecimal storageDetailQty = storageDetail.getQuantityOnHand();

					if (storageDetailQty.compareTo(totalTasksPendingQty) < 0) {
						throw new OBException(
								String.format(OBMessageUtils.messageBD("OBAWO_StorageDetailProtectedByTask"),
										task.getIdentifier(), storageDetailQty, currentTasksPendingQty));
					}
				}
			}
		} finally {
			OBContext.restorePreviousMode();
		}
	}

	/**
	 * Don't allow to update the confirmed fields of a confirmed task
	 */
	private void checkIsConfirmedTask(EntityUpdateEvent event) {
		final Entity taskEntity = entities[0];
		final Property taskStatusProperty = taskEntity.getProperty(OBAWOTask.PROPERTY_STATUS);
		final Property taskConfirmedQtyProperty = taskEntity.getProperty(OBAWOTask.PROPERTY_CONFIRMEDQUANTITY);
		final Property taskConfirmedAttributeProperty = taskEntity.getProperty(OBAWOTask.PROPERTY_CONFIRMEDATTRIBUTE);
		final Property taskConfirmedLocatorFromProperty = taskEntity
				.getProperty(OBAWOTask.PROPERTY_CONFIRMEDLOCATORFROM);
		final Property taskConfirmedLocatorToProperty = taskEntity.getProperty(OBAWOTask.PROPERTY_CONFIRMEDLOCATORTO);

		final String previousIsProcessedStatus = (String) event.getPreviousState(taskStatusProperty);
		final BigDecimal previousQty = (BigDecimal) event.getPreviousState(taskConfirmedQtyProperty);
		final BigDecimal currentQty = (BigDecimal) event.getCurrentState(taskConfirmedQtyProperty);
		final AttributeSetInstance previousAttrib = (AttributeSetInstance) event
				.getPreviousState(taskConfirmedAttributeProperty);
		final AttributeSetInstance currentAttrib = (AttributeSetInstance) event
				.getCurrentState(taskConfirmedAttributeProperty);
		final Locator previousLocatorFrom = (Locator) event.getPreviousState(taskConfirmedLocatorFromProperty);
		final Locator currentLocatorFrom = (Locator) event.getCurrentState(taskConfirmedLocatorFromProperty);
		final Locator previousLocatorTo = (Locator) event.getPreviousState(taskConfirmedLocatorToProperty);
		final Locator currentLocatorTo = (Locator) event.getCurrentState(taskConfirmedLocatorToProperty);

		if (previousIsProcessedStatus.equalsIgnoreCase(OBAWO_Constants.STATUS_CONFIRMED)
				&& (!previousQty.equals(currentQty) || attributesAreDifferent(previousAttrib, currentAttrib)
						|| locatorsAreDifferent(previousLocatorFrom, currentLocatorFrom)
						|| locatorsAreDifferent(previousLocatorTo, currentLocatorTo))) {
			throw new OBAWOException(OBMessageUtils.messageBD("OBAWO_UpdatingProcessedTask"));
		}
	}

	private boolean attributesAreDifferent(final AttributeSetInstance previousAttrib,
			final AttributeSetInstance currentAttrib) {
		return (previousAttrib == null && currentAttrib != null) || (previousAttrib != null && currentAttrib == null)
				|| (previousAttrib != null && currentAttrib != null
						&& !previousAttrib.getId().equals(currentAttrib.getId()));
	}

	private boolean locatorsAreDifferent(final Locator previousLocator, final Locator currentLocator) {
		return (previousLocator == null && currentLocator != null)
				|| (previousLocator != null && currentLocator == null) || (previousLocator != null
						&& currentLocator != null && !previousLocator.getId().equals(currentLocator.getId()));
	}

	/**
	 * Updates the pending picking quantity column in Order Line only for Picking
	 * Sales Order and Issue Sales Order tasks, and updates the Pending Picking
	 * Lines quantity of the Sales Order Header
	 */
	private void updateSalesOrderLinePendingPickingQty(final OBAWOTask task, final EntityPersistenceEvent event) {
		final String ittId = task.getInventoryTransactionType().getId();
		final OrderLine salesOrderLine = task.getSalesOrderLine();
		if (salesOrderLine != null && (StringUtils.equals(ittId, PickingSalesOrder_ITT.INVENTORY_TRANSACTION_TYPE_ID)
				|| (StringUtils.equals(ittId, IssueSalesOrder_ITT.INVENTORY_TRANSACTION_TYPE_ID)
						&& isStandaloneIssueTask(task)))) {
			setPendingQtyPicking(task, event, salesOrderLine);
		}
	}

	/**
	 * The Standalone Issue Tasks are NOT linked to a stock reservation or they are
	 * linked to a stock reservation that has been created manually, not by the task
	 * itself.
	 */
	private boolean isStandaloneIssueTask(final OBAWOTask task) {
		final ReservationStock reservationStock = task.getReservationStock();
		if (reservationStock == null) {
			return true;
		}

		if (!reservationStock.isOBAWOIsCreatedByTask()) {
			try {
				OBContext.setAdminMode(true);
				final String hql = "select 1 " + //
						"from MaterialMgmtInternalMovementLine ml " + //
						"where ml.stockReservation.id = :reservation ";
				final Session session = OBDal.getInstance().getSession();
				final Query<Object> query = session.createQuery(hql, Object.class);
				query.setParameter("reservation", reservationStock.getReservation().getId());
				query.setMaxResults(1);
				return query.uniqueResult() == null ? true : false;
			} finally {
				OBContext.restorePreviousMode();
			}
		}
		return false;
	}

	/**
	 * For EntityNewEvent [for delta tasks]: PendingQtyPicking = PendingQtyPicking -
	 * taskExpectedQty If final Pending Picking Quantity is Zero, then the Pending
	 * Picking Lines of the Order Header is decremented by one
	 * 
	 * For EntityUpdateEvent [when linking the order line to the task at creation
	 * task time]: PendingQtyPicking = PendingQtyPicking - taskExpectedQty <br/>
	 * If final Pending Picking Quantity is Zero, then the Pending Picking Lines of
	 * the Order Header is decremented by one
	 * 
	 * For EntityUpdateEvent [only when confirmed quantity <> expected quantity at
	 * confirmation time]: PendingQtyPicking = PendingQtyPicking + taskExpectedQty -
	 * taskConfirmedQty <br/>
	 * If final Pending Picking Quantity greater than Zero and there are no more
	 * Ongoing Tasks for this Line, then the Pending Picking Lines of the Order
	 * Header is incremented by one
	 * 
	 * For EntityDeleteEvent: PendingQtyPicking = PendingQtyPicking +
	 * taskExpectedQty <br/>
	 * If there are no more Ongoing Tasks for this Line, then the Pending Picking
	 * Lines of the Order Header is incremented by one
	 */
	private void setPendingQtyPicking(final OBAWOTask task, final EntityPersistenceEvent event,
			final OrderLine salesOrderLine) {
		BigDecimal qtyPendingPicking = salesOrderLine.getObawoPendingQtyPicking();

		// For Sales Order Lines created before installation of AWO module, the Quantity
		// Pending Picking
		// is null, in that case, the value is calculated as Ordered Quantity -
		// Delivered Quantity.
		if (qtyPendingPicking == null) {
			qtyPendingPicking = PickingSalesOrder_ITTAlgorithm
					.getQtyPendingToIssueForOrdersGeneratedWithoutAWO(salesOrderLine);
		}
		final Order salesOrder = salesOrderLine.getSalesOrder();
		final Long pendingPickingLines = salesOrder.getOBAWOPendingPickingLines();

		if (event instanceof EntityNewEvent) {
			// This is only for delta tasks
			updatePendingPickingQtyInLinesAndDecrementPickingLinesInHeaderIfTaskFullyPicked(task, salesOrderLine,
					qtyPendingPicking, salesOrder, pendingPickingLines);

		} else if (event instanceof EntityUpdateEvent) {
			final Entity taskEntity = entities[0];
			final Property taskOrderLineProperty = taskEntity.getProperty(OBAWOTask.PROPERTY_SALESORDERLINE);
			final OrderLine oldOrderLine = (OrderLine) ((EntityUpdateEvent) event)
					.getPreviousState(taskOrderLineProperty);
			final OrderLine newOrderLine = (OrderLine) ((EntityUpdateEvent) event)
					.getCurrentState(taskOrderLineProperty);
			if (oldOrderLine == null && newOrderLine != null) {
				// When Linking a Task with an Order Line
				updatePendingPickingQtyInLinesAndDecrementPickingLinesInHeaderIfTaskFullyPicked(task, salesOrderLine,
						qtyPendingPicking, salesOrder, pendingPickingLines);
			} else {
				// At confirmation time
				updatePendingPickingQtyInLinesAndPendingPickingLinesInHeaderWhenConfirmingTask(task, event,
						salesOrderLine, qtyPendingPicking, salesOrder, pendingPickingLines, taskEntity);
			}

		} else if (event instanceof EntityDeleteEvent) {
			salesOrderLine.setObawoPendingQtyPicking(qtyPendingPicking.add(task.getExpectedQuantity()));
			if (noTasksOnGoingForLine(salesOrderLine)) {
				incrementPendingPickingLinesInHeaderByOne(salesOrder, pendingPickingLines);
			}
		}
	}

	private void updatePendingPickingQtyInLinesAndDecrementPickingLinesInHeaderIfTaskFullyPicked(final OBAWOTask task,
			final OrderLine salesOrderLine, final BigDecimal qtyPendingPicking, final Order salesOrder,
			final Long pendingPickingLines) {
		final BigDecimal newQtyPendingPicking = qtyPendingPicking.subtract(task.getExpectedQuantity());
		salesOrderLine.setObawoPendingQtyPicking(newQtyPendingPicking);
		if (newQtyPendingPicking.compareTo(BigDecimal.ZERO) == 0) {
			decrementPendingPickingLinesInHeaderByOne(salesOrder, pendingPickingLines);
		}
	}

	private void updatePendingPickingQtyInLinesAndPendingPickingLinesInHeaderWhenConfirmingTask(final OBAWOTask task,
			final EntityPersistenceEvent event, final OrderLine salesOrderLine, final BigDecimal qtyPendingPicking,
			final Order salesOrder, final Long pendingPickingLines, final Entity taskEntity) {
		final Property taskStatusProperty = taskEntity.getProperty(OBAWOTask.PROPERTY_STATUS);
		final String oldStatus = (String) ((EntityUpdateEvent) event).getPreviousState(taskStatusProperty);
		final String newStatus = (String) ((EntityUpdateEvent) event).getCurrentState(taskStatusProperty);

		// If the update is not to change the status to Confirmed, then return without
		// performing any
		// update to the Pending Picking Qty in Lines or the Pending Picking Lines in
		// the Header
		if (StringUtils.equals(oldStatus, newStatus)
				|| !StringUtils.equals(newStatus, OBAWO_Constants.STATUS_CONFIRMED)) {
			return;
		}

		final Property taskConfirmedQtyProperty = taskEntity.getProperty(OBAWOTask.PROPERTY_CONFIRMEDQUANTITY);
		final BigDecimal newQtyConfirmed = (BigDecimal) ((EntityUpdateEvent) event)
				.getCurrentState(taskConfirmedQtyProperty);
		final BigDecimal qtyDiff = task.getExpectedQuantity().subtract(newQtyConfirmed);

		if (qtyDiff.compareTo(BigDecimal.ZERO) != 0) {
			final BigDecimal newQtyPendingPicking = qtyPendingPicking.add(qtyDiff);
			salesOrderLine.setObawoPendingQtyPicking(newQtyPendingPicking);
			if (newQtyPendingPicking.compareTo(BigDecimal.ZERO) > 0 && noTasksOnGoingForLine(salesOrderLine)) {
				incrementPendingPickingLinesInHeaderByOne(salesOrder, pendingPickingLines);
			}
		}

	}

	/**
	 * Returns true if there are no Tasks in
	 * {@link OBAWO_Constants#STATUS_AVAILABLE} linked to this Order Line
	 */
	private boolean noTasksOnGoingForLine(final OrderLine salesOrderLine) {
		OBCriteria<OBAWOTask> obc = OBDal.getInstance().createCriteria(OBAWOTask.class);
		obc.add(Restrictions.eq(OBAWOTask.PROPERTY_SALESORDERLINE, salesOrderLine));
		obc.add(Restrictions.eq(OBAWOTask.PROPERTY_STATUS, OBAWO_Constants.STATUS_AVAILABLE));
		return obc.count() > 0;
	}

	private void incrementPendingPickingLinesInHeaderByOne(final Order salesOrder, final Long pendingPickingLines) {
		salesOrder.setOBAWOPendingPickingLines(pendingPickingLines + 1L);
	}

	private void decrementPendingPickingLinesInHeaderByOne(final Order salesOrder, final Long pendingPickingLines) {
		salesOrder.setOBAWOPendingPickingLines(pendingPickingLines - 1L);
	}

	private void throwExceptionIfIndividuallyDeletingABehaveAsGroupWithRITask(final OBAWOTask task) {
		if (task.isBehaveAsGroupWithReferencedInventory() && !isCascadeDelete()) {
			throw new OBException(
					String.format(OBMessageUtils.messageBD("OBAWO_Delete_Individual_Task_BehaveAsGroupWithRI"),
							CentralBrokerUtil.getTasksGroupingDocIdentifier(task)));
		}
	}

	private boolean isCascadeDelete() {
		for (final StackTraceElement ste : Thread.currentThread().getStackTrace()) {
			final String classNameInStackTrace = ste.getClassName();
			if (classNameInStackTrace.equals(CASCADE_CLASSNAME)
					|| classNameInStackTrace.equals(JSONDATASERVICEEXTRAACTIONSTASKDELETION_CLASSNAME)
					|| classNameInStackTrace.equals(GENERACIONOPERACIONESPEDIDOS_CLASSNAME)) {
				return true;
			}
		}
		return false;
	}

	private void propagateConfirmedLocatorToOrUserAssignedChangeToRelatedBehaveAsGroupWithRITasks(final OBAWOTask task,
			final EntityUpdateEvent event) {
		if (task.isBehaveAsGroupWithReferencedInventory()
				&& !StringUtils.equals(task.getStatus(), OBAWO_Constants.STATUS_CONFIRMED)) {
			updatePropertyIdIfChanged(task, event, OBAWOTask.PROPERTY_CONFIRMEDLOCATORTO);
			updatePropertyIdIfChanged(task, event, OBAWOTask.PROPERTY_USERCONTACT);

		}
	}

	private void updatePropertyIdIfChanged(final OBAWOTask task, final EntityUpdateEvent event, final String property) {
		final Property taskProperty = entities[0].getProperty(property);
		final String oldValueId = getOldValueId(event, taskProperty);
		final String newValueId = getNewValueId(event, taskProperty);
		if (!StringUtils.equals(oldValueId, newValueId)) {
			runUpdatePropertyIdQuery(task, property, newValueId);
		}
	}

	private String getOldValueId(final EntityUpdateEvent event, final Property property) {
		try {
			return (String) ((BaseOBObject) event.getPreviousState(property)).getId();
		} catch (Exception oldValueIsNull) {
			return null;
		}
	}

	private String getNewValueId(final EntityUpdateEvent event, final Property property) {
		try {
			return (String) ((BaseOBObject) event.getCurrentState(property)).getId();
		} catch (Exception newValueIsNull) {
			return null;
		}
	}

	private void runUpdatePropertyIdQuery(final OBAWOTask task, final String taskProperty, final String newValueId) {
		final String pickingListId = getPickingListId(task);
		final String receptionListId = getReceptionListId(task);

		final StringBuilder hql = new StringBuilder("update OBAWO_Task ");
		hql.append("set " + taskProperty + ".id = :newValueId ");
		hql.append("where id <> :thisTaskId ");
		hql.append("and coalesce(" + taskProperty + ".id, 'X') <> coalesce(:newValueId, 'X') ");
		hql.append("and behaveAsGroupWithReferencedInventory = true ");

		if (!StringUtils.isBlank(pickingListId)) {
			hql.append("and warehousePickingList.id = :pickingListId ");
		} else {
			hql.append("and warehousePickingList.id is null ");
		}

		if (!StringUtils.isBlank(receptionListId)) {
			hql.append("and receptionList.id = :receptionListId ");
		} else {
			hql.append("and receptionList.id is null ");
		}

		@SuppressWarnings("rawtypes")
		final Query query = OBDal.getInstance().getSession().createQuery(hql.toString());
		query.setParameter("newValueId", newValueId);
		if (!StringUtils.isBlank(pickingListId)) {
			query.setParameter("pickingListId", pickingListId);
		}
		if (!StringUtils.isBlank(receptionListId)) {
			query.setParameter("receptionListId", receptionListId);
		}
		query.setParameter("thisTaskId", task.getId());
		if (!StringUtils.isBlank(pickingListId) || !StringUtils.isBlank(receptionListId)) {
			// This check avoids to massively update all tasks in case of bugs setting the
			// picking/reception list
			query.executeUpdate();
		}
	}

	private String getPickingListId(final OBAWOTask task) {
		try {
			return task.getWarehousePickingList().getId();
		} catch (Exception noPickingList) {
			return null;
		}
	}

	private String getReceptionListId(final OBAWOTask task) {
		try {
			return task.getReceptionList().getId();
		} catch (Exception noReceptionList) {
			return null;
		}
	}

	private void updateBinsPendingOccupancyAtTaskCreation(final OBAWOTask task) {
		OccupancyUtils.updateBinsPendingOccupancy(task, TaskEventChangingPendingOccupancyBin.CREATE_TASK);
		// The pending occupancy if Referenced Inventory involved is updated in the
		// BatchOfTasksGenerator later on
	}

	private void updateBinsPendingOccupancyAtIndividualTaskDeletetion(final OBAWOTask task) {
		OccupancyUtils.updateBinsPendingOccupancy(task, TaskEventChangingPendingOccupancyBin.DELETE_TASK);
		// The pending occupancy if Behave as group with Referenced Inventory involved
		// is updated by
		// the GroupOfTasksHeaderObserver instead

		// Here we control deletion of individual tasks where a ref inventory is
		// involved without
		// behaving as a group (for example a box or unbox). In this case a full bins'
		// recalculation
		// must be done. Note we exclude this task because it hasn't been removed yet
		// from the system,
		// so the OccupancyCalculator would find it otherwise
		if (AWOReferencedInventoryUtils.isTaskInvolvedInReferenceInventory(task)
				&& !task.isBehaveAsGroupWithReferencedInventory()) {
			OccupancyUtils.recalculateOccupancyOnlyForBinsInvolvedInReferencedInventory(task, true);
		}
	}
}
