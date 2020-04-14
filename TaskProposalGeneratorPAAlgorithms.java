/*
 ************************************************************************************
 * Copyright (C) 2016-2018 Openbravo S.L.U.
 * Licensed under the Openbravo Commercial License version 1.0
 * You may obtain a copy of the License at http://www.openbravo.com/legal/obcl.html
 * or in the legal folder of this module distribution.
 ************************************************************************************
 */

package org.openbravo.warehouse.advancedwarehouseoperations.task;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.openbravo.dal.service.OBDal;
import org.openbravo.model.common.enterprise.Locator;
import org.openbravo.model.common.plm.Product;
import org.openbravo.model.materialmgmt.onhandquantity.ReferencedInventory;
import org.openbravo.model.materialmgmt.onhandquantity.StorageDetail;
import org.openbravo.warehouse.advancedwarehouseoperations.OBAWOTask;
import org.openbravo.warehouse.advancedwarehouseoperations.occupancy.OccupancyUtils;
import org.openbravo.warehouse.advancedwarehouseoperations.utils.AWOReferencedInventoryUtils;
import org.openbravo.warehouse.advancedwarehouseoperations.utils.Utilities;
import org.openbravo.warehouse.advancedwarehouseoperations.verbosity.AWOVerbosityLevel;
import org.openbravo.warehouse.advancedwarehouseoperations.warehousealgorithm.PA_WarehouseAlgorithm;

/**
 * Creates task proposals locator to using PA Warehouse Algorithms
 * 
 */
class TaskProposalGeneratorPAAlgorithms extends TaskProposalsGeneratorForTaskRequirement {
	final TaskProposalStorageDetail taskProposalStorageDetail;
	BigDecimal qtyPending; // In product's standard precision

	private Set<String> forbiddenStorageBinToIds = new HashSet<>(1);

	TaskProposalGeneratorPAAlgorithms(final TaskRequirement taskRequirement,
			final TaskProposalStorageDetail taskProposalStorageDetail) {
		super(taskRequirement);
		this.taskProposalStorageDetail = taskProposalStorageDetail;
		this.qtyPending = taskProposalStorageDetail.getQty();
		this.forbiddenStorageBinToIds = getStorageBinToIdsPreviouslyDiscardedByDeltaTaskTree(
				taskRequirement.getTaskRequirementsCache().getDeltaTaskId());
	}

	private Set<String> getStorageBinToIdsPreviouslyDiscardedByDeltaTaskTree(final String deltaTaskId) {
		String previousTaskId = deltaTaskId;
		while (StringUtils.isNotBlank(previousTaskId)) {
			final OBAWOTask deltaTask = Utilities.getObjectProxyById(OBAWOTask.ENTITY_NAME, previousTaskId);
			if (deltaTask.getExpectedLocatorTo() != null) {
				forbiddenStorageBinToIds.add(deltaTask.getExpectedLocatorTo().getId());
			}
			try {
				previousTaskId = deltaTask.getDeltafromOBAWOTask().getId();
			} catch (Exception noMoreDelta) {
				previousTaskId = null;
			}
		}
		return forbiddenStorageBinToIds;
	}

	void run() {
		calculateTaskProposalLocatorTo();
	}

	private void calculateTaskProposalLocatorTo() {
		if (AWOReferencedInventoryUtils.mustKeepStorageDetailInsideReferencedInventory(taskRequirement)) {
			createReferencedInventoryTaskProposalLocatorFromCacheStorageBinIfAvailable();
		}
		loopOverPAAlgorithmsByPriorityAndCreateTaskProposalsForPendingQty();
		createTaskProposalWithEmptyLocatorForPendingQty();
	}

	private void createReferencedInventoryTaskProposalLocatorFromCacheStorageBinIfAvailable() {
		final String refInventoryId = taskRequirement.getReferencedInventory().getId();
		final String refInventoryPreviouslyProposedBinId = taskRequirement.getTaskRequirementsCache()
				.getRefInventoryProposedBin(refInventoryId);
		if (!StringUtils.isBlank(refInventoryPreviouslyProposedBinId)) {
			final Locator locator = OBDal.getInstance().getProxy(Locator.class, refInventoryPreviouslyProposedBinId);
			logLine(AWOVerbosityLevel.INFO,
					"Detected Locator To [%s] previously proposed for the referenced inventory [%s].", locator,
					OBDal.getInstance().getProxy(ReferencedInventory.class, refInventoryId));
			createTaskProposalLocatorTo_UpdateQtyPending_UpdateRefInvProposedBinCache(null, locator, qtyPending);
		}
	}

	private void loopOverPAAlgorithmsByPriorityAndCreateTaskProposalsForPendingQty() {
		if (isPositive(qtyPending)) {
			final List<String> paWarehouseAlgorithms = WarehouseAlgorithmsUtils
					.getPAWarehouseAlgorithmsOrderByPriority(taskRequirement);
			int i = 0;
			while (i < paWarehouseAlgorithms.size() && isPositive(qtyPending)) {
				final String paWarehouseAlgorithm = paWarehouseAlgorithms.get(i);
				final PA_WarehouseAlgorithm paWarehouseAlgorithmInstance = (PA_WarehouseAlgorithm) WarehouseAlgorithmsUtils
						.getWarehouseAlgorithmInstance(taskRequirement, paWarehouseAlgorithm);
				distributeStockUsingAlgorithm(paWarehouseAlgorithmInstance);
				i++;
			}
		}
	}

	private void distributeStockUsingAlgorithm(final PA_WarehouseAlgorithm paWarehouseAlgoritmInstance) {
		final Locator storageBinFrom = getStorageBinFrom();
		final List<String> storageBinToIdList = getCalculatedLocatorToList(paWarehouseAlgoritmInstance);

		int i = 0;
		while (isPositive(qtyPending) && i < storageBinToIdList.size()) {
			final String storageBinToId = storageBinToIdList.get(i++);
			final Locator storageBinTo = Utilities.getObjectProxyById(Locator.ENTITY_NAME, storageBinToId);
			logLine(AWOVerbosityLevel.INFO, "Trying to use bin [%s] returned by PA Algorithm", storageBinTo);

//			StorageDetail stock = this.taskRequirement.getStorageDetail();

//			if (stock != null && stock.getReferencedInventory() != null
//					&& areTheSameBin(storageBinFrom, storageBinTo)) {
//				logLine(AWOVerbosityLevel.INFO, "Proposed bin to is the same as the original bin from. Skipping it.");
//				continue;
//			}

			if (forbiddenStorageBinToIds.contains(storageBinToId)) {
				logLine(AWOVerbosityLevel.INFO,
						"Proposed bin to [%s] is was discarded by a previous delta task. Skipping it.",
						Utilities.getObjectProxyById(Locator.ENTITY_NAME, storageBinToId));
				continue;
			}

			final BigDecimal allocatedQty = getQtyToBeAllocate(storageBinTo, paWarehouseAlgoritmInstance);
			createTaskProposalLocatorTo_UpdateQtyPending_UpdateRefInvProposedBinCache(
					paWarehouseAlgoritmInstance.getWarehouseAlgorithmId(), storageBinTo, allocatedQty);

			Utilities.removeObjectFromHibernateSession(storageBinTo);
		}
	}

	private Locator getStorageBinFrom() {
		final StorageDetail storageDetail = taskProposalStorageDetail.getStorageDetail();
		return storageDetail == null ? null : storageDetail.getStorageBin();
	}

	private List<String> getCalculatedLocatorToList(final PA_WarehouseAlgorithm paWarehouseAlgoritmInstance) {
		final List<String> storageBinToIdList = paWarehouseAlgoritmInstance.calculateLocatorToList(taskRequirement);
		if (storageBinToIdList.isEmpty()) {
			logLine(AWOVerbosityLevel.INFO, "No Bin has been returned by the PA Warehouse Algorithm.");
		} else {
			logLine(AWOVerbosityLevel.DEBUG, "To-Bin Ids returned by the PA Warehouse Algorithm: [%s]",
					storageBinToIdList);
		}
		return storageBinToIdList;
	}

	private boolean areTheSameBin(Locator storageBinFrom, Locator storageBinTo) {
		if (storageBinFrom == null || storageBinTo == null) {
			return false;
		} else {
			return storageBinFrom.getId().equals(storageBinTo.getId());
		}
	}

	private BigDecimal getQtyToBeAllocate(final Locator storageBinTo,
			final PA_WarehouseAlgorithm paWarehouseAlgoritmInstance) {
		BigDecimal qtyToBeAllocated;
		if (isInFunctionalArea(storageBinTo)) {
			qtyToBeAllocated = qtyPending;
			logLine(AWOVerbosityLevel.DEBUG,
					"Bin To [%s] is in a Functional Area, so it can allocate full pending quantity [%s]", storageBinTo,
					qtyToBeAllocated);
		} else {
			logLine(AWOVerbosityLevel.DEBUG, "Bin To is in a Non-Functional Area. Taking into account capacity...");
			qtyToBeAllocated = calculateQtyToBeAllocatedWithCapacity(storageBinTo);
		}
		// Call method that can be extended by subclasses to modify the allocated
		// Quantity
		qtyToBeAllocated = getQtyToGenerateTaskProposalLocatorTo(paWarehouseAlgoritmInstance, qtyToBeAllocated,
				storageBinTo);

		return qtyToBeAllocated;
	}

	private Boolean isInFunctionalArea(final Locator storageBin) {
		return storageBin.getOBAWOLocatorGroup().getInternalRoutingArea().isFunctional();
	}

	private BigDecimal calculateQtyToBeAllocatedWithCapacity(final Locator storageBinTo) {
		final String productId = getProductIdConsideringReferencedInventory();
		final BigDecimal availableCapacityForProduct = OccupancyUtils
				.getAvailableCapacityForBinAndProduct(storageBinTo.getId(), productId);
		logLine(AWOVerbosityLevel.INFO, "Available Capacity in the Storage Bin: %s for Product %s is: %s", storageBinTo,
				OBDal.getInstance().getProxy(Product.class, productId), availableCapacityForProduct);

		final BigDecimal previouslyAllocatedQty = getPreviouslyAllocatedQtyInBin(storageBinTo);
		logLine(AWOVerbosityLevel.DEBUG, "Previously allocated quantity in bin to [%s] is [%s]", storageBinTo,
				previouslyAllocatedQty);

		final BigDecimal qtyToBeAllocated = getQtyToBeAllocatedConsideringReferencedInventory(storageBinTo,
				availableCapacityForProduct, previouslyAllocatedQty);

		logLine(AWOVerbosityLevel.DEBUG, "Quantity that can be allocated is [%s]", qtyToBeAllocated);
		return qtyToBeAllocated;
	}

	private String getProductIdConsideringReferencedInventory() {
		final String productId;
		if (AWOReferencedInventoryUtils.mustKeepStorageDetailInsideReferencedInventory(taskRequirement)) {
			final Product product = taskRequirement.getReferencedInventoryProduct();
			logLine(AWOVerbosityLevel.INFO,
					"Provided Storage Detail is inside a Referenced Inventory, so we must fully allocate it in one bin. "
							+ "Using product [%s] to calculate capacity",
					product);
			productId = product.getId();
		} else {
			productId = taskRequirement.getProductId();
		}
		return productId;
	}

	private BigDecimal getPreviouslyAllocatedQtyInBin(Locator storageBinTo) {
		return taskRequirement.getTaskRequirementsCache().getAllocatedInBin(storageBinTo.getId());
	}

	private BigDecimal getQtyToBeAllocatedConsideringReferencedInventory(final Locator storageBinTo,
			final BigDecimal availableCapacityForProduct, final BigDecimal previouslyAllocatedQty) {
		final BigDecimal qtyToBeAllocated;
		if (AWOReferencedInventoryUtils.mustKeepStorageDetailInsideReferencedInventory(taskRequirement)) {
			final BigDecimal qtyRefInvToBeAllocated = availableCapacityForProduct.subtract(previouslyAllocatedQty)
					.max(BigDecimal.ZERO);
			if (qtyRefInvToBeAllocated.compareTo(BigDecimal.ONE) < 0) { // Split would be needed
				logLine(AWOVerbosityLevel.INFO,
						"Proposed bin [%s] has not enough quantity to fully allocate the referenced inventory. Try to find another bin.",
						storageBinTo);
				qtyToBeAllocated = BigDecimal.ZERO;
			} else {
				qtyToBeAllocated = qtyPending;
			}
		} else {
			qtyToBeAllocated = qtyPending.min(availableCapacityForProduct.subtract(previouslyAllocatedQty))
					.max(BigDecimal.ZERO);
		}
		return qtyToBeAllocated;
	}

	private BigDecimal getQtyToGenerateTaskProposalLocatorTo(final PA_WarehouseAlgorithm paWarehouseAlgoritmInstance,
			final BigDecimal qtyToBeAllocated, final Locator storageBinTo) {
		return paWarehouseAlgoritmInstance.getPossibleAllocatedQtyInTaskInStdPrecision(qtyToBeAllocated, storageBinTo);
	}

	private void createTaskProposalLocatorTo_UpdateQtyPending_UpdateRefInvProposedBinCache(
			final String warehouseAlgorithmId, final Locator storageBinTo, final BigDecimal allocatedQty) {
		if (allocatedQty.compareTo(BigDecimal.ZERO) != 0) {
			if (AWOReferencedInventoryUtils.mustKeepStorageDetailInsideReferencedInventory(taskRequirement)) {
				qtyPending = BigDecimal.ZERO; // Everything must be allocated here
			} else {
				qtyPending = qtyPending.subtract(allocatedQty);
			}
			logLine(AWOVerbosityLevel.INFO,
					"Allocating [%s] of product [%s] in Storage Bin [%s]. Pending Stock to be allocated afterwards [%s]",
					allocatedQty, taskRequirement.getProduct(), storageBinTo, qtyPending);
			setRefInventoryProposedBinCache(storageBinTo);
			new TaskProposalLocatorTo(taskProposalStorageDetail, allocatedQty, storageBinTo.getId(),
					warehouseAlgorithmId);
		}
	}

	private void setRefInventoryProposedBinCache(final Locator storageBinTo) {
		if (AWOReferencedInventoryUtils.mustKeepStorageDetailInsideReferencedInventory(taskRequirement)) {
			taskRequirement.getTaskRequirementsCache()
					.setRefInventoryProposedBin(taskRequirement.getReferencedInventory().getId(), storageBinTo.getId());
		}
	}

	private void createTaskProposalWithEmptyLocatorForPendingQty() {
		if (isPositive(qtyPending)) {
			logLine(AWOVerbosityLevel.INFO, "The algorithms have not been able to allocate all the stock. "
					+ "Therefore, a new task with empty Storage To-Bin is going to be created for the remaining quantity: "
					+ qtyPending);
			new TaskProposalLocatorTo(taskProposalStorageDetail, qtyPending, null, null);
		}
	}
}
