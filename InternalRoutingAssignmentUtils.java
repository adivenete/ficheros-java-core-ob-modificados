/*
 ************************************************************************************
 * Copyright (C) 2017-2019 Openbravo S.L.U.
 * Licensed under the Openbravo Commercial License version 1.0
 * You may obtain a copy of the License at http://www.openbravo.com/legal/obcl.html
 * or in the legal folder of this module distribution.
 ************************************************************************************
 */

package org.openbravo.warehouse.advancedwarehouseoperations.task;

import java.util.List;

import org.hibernate.Session;
import org.hibernate.query.Query;
import org.openbravo.base.exception.OBException;
import org.openbravo.dal.core.OBContext;
import org.openbravo.dal.service.OBDal;
import org.openbravo.erpCommon.utility.OBMessageUtils;
import org.openbravo.model.common.plm.Product;
import org.openbravo.warehouse.advancedwarehouseoperations.OBAWOInternalRoutingAssi;
import org.openbravo.warehouse.advancedwarehouseoperations.verbosity.AWOVerbosityLevel;

import es.opentix.awo.ampliacion.interfaces.RoutingAssignementAlgorithmInterface;
import es.opentix.devpro.OpxLog;

class InternalRoutingAssignmentUtils {

	private static final int MATCH_PRODUCT = 16;
	private static final int MATCH_PRODUCT_CATEGORY = 8;
	private static final int MATCH_BUSINESSPARTNER = 4;
	private static final int MATCH_BUSINESSPARTNER_CATEGORY = 2;
	private static final int MATCH_STORAGEBINGROUP = 1;
	private static final int NOMATCH = 0;

	/**
	 * Returns the Routing Assignment for the given task requirement based on the
	 * additional filter rules:
	 * <ul>
	 * <li>Concrete product match: +{@value #MATCH_PRODUCT}</li>
	 * <li>Concrete product category match: +{@value #MATCH_PRODUCT_CATEGORY}</li>
	 * <li>Concrete business partner match: +{@value #MATCH_BUSINESSPARTNER}</li>
	 * <li>Concrete business partner category match:
	 * +{@value #MATCH_BUSINESSPARTNER_CATEGORY}</li>
	 * <li>Concrete storage bin group match: +{@value #MATCH_STORAGEBINGROUP}</li>
	 * </ul>
	 * 
	 * Throws an exception if no Routing Assignment found
	 */
	static OBAWOInternalRoutingAssi calculateInternalRoutingAssignment(final TaskRequirement taskRequirement) {

		logStart(taskRequirement);
		try {
			OBContext.setAdminMode(true);
			// @formatter:off
			final StringBuffer hql = new StringBuffer();
			hql.append("select iras ");
			hql.append("from OBAWO_Internal_Routing_Assi iras ");
			hql.append("inner join iras.internalRouting ir ");
			if (!taskRequirement.isSearchStorageDetail()) {
				hql.append("left join ir.internalRoutingAreaFrom irareaf ");
			}
			if (!taskRequirement.isSearchLocatorTo()) {
				hql.append("left join ir.internalRoutingAreaTo irareat ");
			}
			hql.append("where iras.obawoInventoryTransactionType.id = :inventoryTransactionTypeId ");
			hql.append("and iras.warehouse.id = :warehouseId ");
			hql.append("and (iras.product.id = :productId or iras.product is null) ");
			hql.append("and (iras.productCategory.id = :productCategoryId or iras.productCategory is null) ");
			hql.append("and (iras.businessPartner.id = :businessPartnerId or iras.businessPartner is null) ");
			hql.append(
					"and (iras.businessPartnerCategory.id = :bpartnerCategoryId or iras.businessPartnerCategory is null) ");
			if (!taskRequirement.isSearchStorageDetail()) {
				hql.append("and (irareaf.id = :irareafId or irareaf is null) ");
			}
			if (!taskRequirement.isSearchLocatorTo()) {
				hql.append("and (irareat.id = :irareatId or irareat is null) ");
			}
			hql.append("and (iras.storageBinGroup.id = :storageBinGroupId or iras.storageBinGroup is null) ");
			hql.append("and iras.active = true and ir.active = true ");
			hql.append("order by ");
			// =============================================================
			// MODIFIED BY xisco on 15 nov. 2018.
			// Description: Primero se rdena por orden de secuencia
			hql.append("iras.opxaawoSecuencia asc, ");
			// END OF MODIFICATION
			// =============================================================
			hql.append("  (case when iras.product.id = :productId then " + MATCH_PRODUCT + " else " + NOMATCH
					+ " end) + ");
			hql.append("  (case when iras.productCategory.id = :productCategoryId then " + MATCH_PRODUCT_CATEGORY
					+ " else " + NOMATCH + " end) + ");
			hql.append("  (case when iras.businessPartner.id = :businessPartnerId then " + MATCH_BUSINESSPARTNER
					+ " else " + NOMATCH + " end) + ");
			hql.append("  (case when iras.businessPartnerCategory.id = :bpartnerCategoryId then "
					+ MATCH_BUSINESSPARTNER_CATEGORY + " else " + NOMATCH + " end) + ");
			hql.append("  (case when iras.storageBinGroup.id = :storageBinGroupId then " + MATCH_STORAGEBINGROUP
					+ " else " + NOMATCH + " end) DESC ");

			// @formatter:on
			final Session session = OBDal.getInstance().getSession();
			final Query<OBAWOInternalRoutingAssi> query = session.createQuery(hql.toString(),
					OBAWOInternalRoutingAssi.class);
			query.setParameter("inventoryTransactionTypeId", taskRequirement.getInventoryTransactionTypeId());
			query.setParameter("warehouseId", taskRequirement.getWarehouseId());
			Product product = ProductConsideringReferencedInventoryUtils
					.getProductConsideringReferencedInventory(taskRequirement);

			if (taskRequirement.getInventoryTransactionType().isBehaveasgroup()
					&& taskRequirement.getStorageDetail() != null
					&& taskRequirement.getStorageDetail().getReferencedInventory() != null) {
				product = ProductConsideringReferencedInventoryUtils
						.getProductConsideringReferencedInventory(taskRequirement);
			} else {
				product = taskRequirement.getProduct();
			}

			query.setParameter("productId", product.getId());
			query.setParameter("productCategoryId", product.getProductCategory().getId());
			query.setParameter("businessPartnerId", taskRequirement.getBusinessPartnerId());
			query.setParameter("bpartnerCategoryId", taskRequirement.getBpCategoryId());
			if (!taskRequirement.isSearchStorageDetail()) {
				query.setParameter("irareafId", taskRequirement.getInternalRoutingAreaFromId());
			}
			if (!taskRequirement.isSearchLocatorTo()) {
				query.setParameter("irareatId", taskRequirement.getInternalRoutingAreaToId());
			}

			// storageBinGroupId doesn't make sense in RECEIPTS
			query.setParameter("storageBinGroupId", taskRequirement.getStorageBinGroupFromId());

			// =============================================================
			// MODIFIED BY xisco on 15 nov. 2018.
			// Description:

			List<OBAWOInternalRoutingAssi> internalRoutingAssignmentReducedList = query.list();

			if (taskRequirement.getAttributeSetInstance() != null && taskRequirement.getStorageDetail() != null) {

				for (OBAWOInternalRoutingAssi ruta : internalRoutingAssignmentReducedList) {
					try {
						if (ruta.getOpxaawoRaalgorithm() != null) {
							Class<?> clase = Class.forName(ruta.getOpxaawoRaalgorithm().getClaseJava());
							RoutingAssignementAlgorithmInterface nuevaExtension = (RoutingAssignementAlgorithmInterface) clase
									.newInstance();
							boolean algoritmoCorrecto = nuevaExtension.executeAlgorithmRefInventoryProducts(
									taskRequirement, taskRequirement.getStorageDetail(), ruta);
							if (algoritmoCorrecto) {
								taskRequirement.logLine(AWOVerbosityLevel.DEBUG, "Query: [%s]", query.getQueryString());
								taskRequirement.logLine(AWOVerbosityLevel.INFO,
										internalRoutingAssignmentReducedList.size() > 1
												? "Detected more than one Routing Assignment, using [%s]"
												: "Using the unique Routing Assignment found [%s]",
										ruta);
								return ruta;
							}
						} else {
							final OBAWOInternalRoutingAssi internalRoutingAssignment = ruta;
							taskRequirement.logLine(AWOVerbosityLevel.DEBUG, "Query: [%s]", query.getQueryString());
							taskRequirement.logLine(AWOVerbosityLevel.INFO,
									internalRoutingAssignmentReducedList.size() > 1
											? "Detected more than one Routing Assignment, using [%s]"
											: "Using the unique Routing Assignment found [%s]",
									internalRoutingAssignment);
							return internalRoutingAssignment;
						}

					} catch (ClassNotFoundException e) {
						OpxLog.exception(InternalRoutingAssignmentUtils.class, e);
						throw new OBException("No se ha encontrado la clase: "
								+ ruta.getOpxaawoRaalgorithm().getClaseJava() + ". " + e.getMessage());
					} catch (ClassCastException e) {
						OpxLog.exception(InternalRoutingAssignmentUtils.class, e);
						throw new OBException("La clase " + ruta.getOpxaawoRaalgorithm().getClaseJava()
								+ " no está definida correctamente. " + e.getMessage());
					} catch (InstantiationException e) {
						OpxLog.exception(InternalRoutingAssignmentUtils.class, e);
						throw new OBException("La clase " + ruta.getOpxaawoRaalgorithm().getClaseJava()
								+ " no ha podido ser instanciada. " + e.getMessage());
					} catch (IllegalAccessException e) {
						OpxLog.exception(InternalRoutingAssignmentUtils.class, e);
						throw new OBException("No se puede acceder a la clase "
								+ ruta.getOpxaawoRaalgorithm().getClaseJava() + ". " + e.getMessage());
					} catch (Exception e) {
						throw new OBException(e.getMessage());
					}
				}
			} else {

				for (OBAWOInternalRoutingAssi ruta : internalRoutingAssignmentReducedList) {
					// Instanciamos cada una de las clases java.
					try {
						if (ruta.getOpxaawoRaalgorithm() != null) {
							Class<?> clase = Class.forName(ruta.getOpxaawoRaalgorithm().getClaseJava());
							RoutingAssignementAlgorithmInterface nuevaExtension = (RoutingAssignementAlgorithmInterface) clase
									.newInstance();
							boolean algoritmoCorrecto = nuevaExtension.executeAlgorithm(taskRequirement, ruta);
							if (algoritmoCorrecto) {
								taskRequirement.logLine(AWOVerbosityLevel.DEBUG, "Query: [%s]", query.getQueryString());
								taskRequirement.logLine(AWOVerbosityLevel.INFO,
										internalRoutingAssignmentReducedList.size() > 1
												? "Detected more than one Routing Assignment, using [%s]"
												: "Using the unique Routing Assignment found [%s]",
										ruta);
								return ruta;
							}
						} else {

							// final OBAWOInternalRoutingAssi internalRoutingAssignment =
							// internalRoutingAssignmentReducedList.get(0);
							final OBAWOInternalRoutingAssi internalRoutingAssignment = ruta;
							taskRequirement.logLine(AWOVerbosityLevel.DEBUG, "Query: [%s]", query.getQueryString());
							taskRequirement.logLine(AWOVerbosityLevel.INFO,
									internalRoutingAssignmentReducedList.size() > 1
											? "Detected more than one Routing Assignment, using [%s]"
											: "Using the unique Routing Assignment found [%s]",
									internalRoutingAssignment);
							return internalRoutingAssignment;

						}

					} catch (ClassNotFoundException e) {
						OpxLog.exception(InternalRoutingAssignmentUtils.class, e);
						throw new OBException("No se ha encontrado la clase: "
								+ ruta.getOpxaawoRaalgorithm().getClaseJava() + ". " + e.getMessage());
					} catch (ClassCastException e) {
						OpxLog.exception(InternalRoutingAssignmentUtils.class, e);
						throw new OBException("La clase " + ruta.getOpxaawoRaalgorithm().getClaseJava()
								+ " no está definida correctamente. " + e.getMessage());
					} catch (InstantiationException e) {
						OpxLog.exception(InternalRoutingAssignmentUtils.class, e);
						throw new OBException("La clase " + ruta.getOpxaawoRaalgorithm().getClaseJava()
								+ " no ha podido ser instanciada. " + e.getMessage());
					} catch (IllegalAccessException e) {
						OpxLog.exception(InternalRoutingAssignmentUtils.class, e);
						throw new OBException("No se puede acceder a la clase "
								+ ruta.getOpxaawoRaalgorithm().getClaseJava() + ". " + e.getMessage());
					} catch (Exception e) {
						throw new OBException(e.getMessage());
					}

				}
			}
			throw new OBException(String.format(OBMessageUtils.messageBD("OBAWO_NoInternalRoutingAssignmentFound"),
					taskRequirement.toString()));
			// END OF MODIFICATION
			// =============================================================

		} finally {
			OBContext.restorePreviousMode();
		}
	}

	private static void logStart(final TaskRequirement taskRequirement) {

		taskRequirement.logLine(AWOVerbosityLevel.DEBUG,
				"Getting Routing Assignment for inventoryTransactionTypeId [%s], warehouseId [%s], productId [%s], productCategoryId [%s], businessPartnerId [%s], bpartnerCategoryId [%s], storageBinGroupId [%s], irareaId [%s]",
				taskRequirement.getInventoryTransactionTypeId(), taskRequirement.getWarehouseId(),
				taskRequirement.getProductId(), taskRequirement.getProductCategoryId(),
				taskRequirement.getBusinessPartnerId(), taskRequirement.getBpCategoryId(),
				taskRequirement.getStorageBinGroupFromId(),
				(!taskRequirement.isSearchStorageDetail() ? taskRequirement.getInternalRoutingAreaFromId()
						: (!taskRequirement.isSearchLocatorTo() ? taskRequirement.getInternalRoutingAreaToId()
								: null)));

	}

}
