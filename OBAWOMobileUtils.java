/*
 ************************************************************************************
 * Copyright (C) 2016-2018 Openbravo S.L.U.
 * Licensed under the Openbravo Commercial License version 1.0
 * You may obtain a copy of the License at http://www.openbravo.com/legal/obcl.html
 * or in the legal folder of this module distribution.
 ************************************************************************************
 */

package org.openbravo.warehouse.advancedwarehouseoperations.mobile;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.hibernate.criterion.Restrictions;
import org.openbravo.base.exception.OBException;
import org.openbravo.base.model.Entity;
import org.openbravo.base.model.ModelProvider;
import org.openbravo.base.provider.OBProvider;
import org.openbravo.base.session.OBPropertiesProvider;
import org.openbravo.dal.core.OBContext;
import org.openbravo.dal.service.OBCriteria;
import org.openbravo.dal.service.OBDal;
import org.openbravo.dal.service.OBQuery;
import org.openbravo.erpCommon.utility.OBMessageUtils;
import org.openbravo.mobile.core.process.JSONPropertyToEntity;
import org.openbravo.mobile.core.utils.OBMOBCUtils;
import org.openbravo.model.common.enterprise.Warehouse;
import org.openbravo.model.common.plm.Attribute;
import org.openbravo.model.common.plm.AttributeInstance;
import org.openbravo.model.common.plm.AttributeSetInstance;
import org.openbravo.model.common.plm.AttributeUse;
import org.openbravo.model.common.plm.AttributeValue;
import org.openbravo.model.common.plm.Product;
import org.openbravo.model.materialmgmt.onhandquantity.ReferencedInventory;
import org.openbravo.warehouse.advancedwarehouseoperations.OBAWOTask;
import org.openbravo.warehouse.advancedwarehouseoperations.utils.AWOAttributeLifeCycleUtils;
import org.openbravo.warehouse.advancedwarehouseoperations.utils.AWOReferencedInventoryUtils;
import org.openbravo.warehouse.advancedwarehouseoperations.utils.OBAWO_Constants;

public class OBAWOMobileUtils {
	private static final Logger logger = Logger.getLogger(OBAWOMobileUtils.class);

	/**
	 * Receives an array from mobile app with task an writes these values to the DB
	 * 
	 * Throws OBException if not possible to process
	 * 
	 * @param JSONArray Tasks
	 * @throws JSONException
	 */
	public static void copyTaskFromMobileToServer(JSONArray tasks) throws JSONException {
		for (int i = 0; i < tasks.length(); i++) {
			JSONObject jsonCurrentTask = tasks.getJSONObject(i);
			copyTaskFromMobileToServer(jsonCurrentTask);
			if (i % 100 == 0) {
				OBDal.getInstance().flush();
			}
		}
		OBDal.getInstance().flush();
	}

	/**
	 * Receives a task from mobile app an writes its values to the DB
	 * 
	 * Throws OBException if not possible to process
	 * 
	 * @param JSONObject
	 * @throws JSONException
	 */
	public static void copyTaskFromMobileToServer(JSONObject jsonTask) throws JSONException {
		// Si el producto tiene atributo
		// Si la transacción permite cambio de atributo
		// se ejecuta la logica de cambio de atributo
		Entity taskEntity = ModelProvider.getInstance().getEntity(OBAWOTask.class);
		OBAWOTask curTask = OBDal.getInstance().get(OBAWOTask.class, jsonTask.getString("id"));
		String baseTaskTypeSk = curTask.getTaskType().getBaseTaskType().getSearchKey();
		JSONPropertyToEntity.fillBobFromJSON(taskEntity, curTask, jsonTask, null);
		if (isAttributeChangeAllowedForTask(curTask, jsonTask)) {
			AttributeSetInstance newAttSetInst = null;
			// AWOAttributeLifeCycleUtils.cloneAttributeSetInstance
//      if (baseTaskTypeSk.equals(OBAWO_Constants.BTT_RECEIPT)) {
			// Here 2 things can happen:
			// if product has a default AttSetValue -> expected Atts are not null
			// if product doesen't have any default attSetValue -> Expected Atts are null
			if (curTask.getExpectedAttribute() == null) {
				// When it is null, it is clear that we need to create a new AttSetIntanceId
				newAttSetInst = createAttributeSetInstance(curTask.getProduct(), jsonTask);
			} else {
				// Product has expected. We need to check every attribute. If just one is
				// different to the
				// defaults a new attribute set instance must be created
				Boolean areAttsDifferent = false;
				if (areAttsDifferent) {
					// there are changes -> create new attSetInstance
					newAttSetInst = createAttributeSetInstance(curTask.getProduct(), jsonTask);
				} else {
					// no changes
					newAttSetInst = curTask.getExpectedAttribute();
				}
			}
//      } else {
//        // Attribute change is allowed. There are expected attribute values but we need to check
//        // if confirmed values are the same or not. To do that we will call to an API function
//        // which will decide if a new attribute set instance should be created or selected.
//        // To be able to call to this util, a map with attribute values will be created.
//        newAttSetInst = AWOAttributeLifeCycleUtils.cloneAttributeSetInstance(
//            curTask.getExpectedAttribute(),
//            createAttributeMapBasedOnConfirmedAttributesValues(jsonTask, false));
//      }
			curTask.setConfirmedAttribute(newAttSetInst);
		}
		if (OBMOBCUtils.isJsonObjectPropertyStringPresentNotNullAndNotEmptyString(jsonTask,
				"confirmedRefInv_searchkey")) {
			// In case that ref inv is received, pick the new one.
			ReferencedInventory confirmedReferenceInventory = AWOReferencedInventoryUtils
					.getReferenceBySearchKey(jsonTask.getString("confirmedRefInv_searchkey"));
			if (confirmedReferenceInventory != null) {
				curTask.setConfirmedMRefinventory(confirmedReferenceInventory);
			} else {
				String[] errorMsgArgs = { jsonTask.getString("confirmedRefInv_searchkey") };
				String errorMsg = OBMessageUtils.getI18NMessage("OBAWO_ReferenceNotExists", errorMsgArgs);
				throw new OBException(errorMsg, true);
			}
		} else {
			curTask.setConfirmedMRefinventory(null);
		}

	}

	public static AttributeSetInstance createAttributeSetInstance(Product product, JSONObject jsonObject)
			throws JSONException {

		if (product.getAttributeSet() != null) {
			AttributeSetInstance attSetInst = null;

			if (jsonObject.has("confirmedAttValues") && !jsonObject.isNull("confirmedAttValues")) {
				try {
					// Atts are created in the organization of the product. Make sense to skip org
					// permissions
					OBContext.setAdminMode(false);
					final JSONObject jsonConfirmedAttValues = jsonObject.getJSONObject("confirmedAttValues");
					final String serialNo = jsonConfirmedAttValues.has("serialno")
							? jsonConfirmedAttValues.getJSONObject("serialno").getString("value")
							: "";
					final String lot = jsonConfirmedAttValues.has("lot")
							? jsonConfirmedAttValues.getJSONObject("lot").getString("value")
							: "";
					final String strDate = jsonConfirmedAttValues.has("guaranteedate")
							? jsonConfirmedAttValues.getJSONObject("guaranteedate").getString("value")
							: "";
					Date expirationDate = null;

					if (!strDate.isEmpty()) {
						try {
							final String date = strDate.indexOf("T") < 0 ? strDate
									: strDate.substring(0, strDate.indexOf("T"));
							expirationDate = new SimpleDateFormat("yyyy-MM-dd").parse(date);
						} catch (Exception e) {
							logger.error(e.getMessage(), e);
						}
					}

					attSetInst = OBProvider.getInstance().get(AttributeSetInstance.class);
					attSetInst.setClient(product.getClient());
					attSetInst.setOrganization(product.getOrganization());
					attSetInst.setAttributeSet(product.getAttributeSet());
					attSetInst.setSerialNo(serialNo);
					attSetInst.setLotName(lot);
					attSetInst.setExpirationDate(expirationDate);

					OBDal.getInstance().save(attSetInst);

					final OBCriteria<AttributeUse> attributeuseCriteria = OBDal.getInstance()
							.createCriteria(AttributeUse.class);
					attributeuseCriteria
							.add(Restrictions.eq(AttributeUse.PROPERTY_ATTRIBUTESET, product.getAttributeSet()));
					attributeuseCriteria.addOrderBy(AttributeUse.PROPERTY_SEQUENCENUMBER, true);

					final List<AttributeUse> attributeUseResults = attributeuseCriteria.list();
					String attSetInstanceDescription = "";
					for (AttributeUse attUse : attributeUseResults) {
						String attValue = null;
						Attribute att = attUse.getAttribute();

						try {
							attValue = OBAWOMobileUtils.getAttributeValue(jsonConfirmedAttValues, att);
						} catch (JSONException e) {
							logger.error("An error happened while reading att values for att " + att.getIdentifier()
									+ ". " + e.getMessage());
							throw new OBException("An error happened while reading att values for att "
									+ att.getIdentifier() + ". " + e.getMessage());
						}

						if (att.isMandatory() && attValue == null) {
							throw new JSONException("Attribute (" + att.getIdentifier() + "is not present in json");
						}

						if (attValue != null) {
							attSetInstanceDescription = attSetInstanceDescription.isEmpty() ? attValue
									: attSetInstanceDescription + "_" + attValue;
							final AttributeInstance attInstance = (AttributeInstance) OBProvider.getInstance()
									.get(AttributeInstance.ENTITY_NAME);
							Attribute currentAttribute = attUse.getAttribute();
							attInstance.setAttribute(currentAttribute);
							attInstance.setSearchKey(attValue);
							attInstance.setAttributeSetValue(attSetInst);
							if (currentAttribute.isList()) {
								OBQuery<AttributeValue> attValueQuery = OBDal.getInstance()
										.createQuery(AttributeValue.class, "attribute.id = :attId and name = :value");
								attValueQuery.setNamedParameter("value", attValue);
								attValueQuery.setNamedParameter("attId", currentAttribute.getId());
								attValueQuery.setFetchSize(1);
								AttributeValue attValueFound = attValueQuery.uniqueResult();
								if (attValueFound != null) {
									attInstance.setAttributeValue(attValueFound);
								}
							}

							OBDal.getInstance().save(attInstance);
						}
					}

					if (lot != null && !lot.isEmpty()) {
						attSetInstanceDescription = attSetInstanceDescription.isEmpty() ? "L" + lot
								: attSetInstanceDescription + "_L" + lot;
					}
					if (serialNo != null && !serialNo.isEmpty()) {
						attSetInstanceDescription = attSetInstanceDescription.isEmpty() ? "#" + serialNo
								: attSetInstanceDescription + "_#" + serialNo;
					}
					if (expirationDate != null) {
						final String javaFormat = (String) OBPropertiesProvider.getInstance().getOpenbravoProperties()
								.get("dateFormat.java");
						final String strExpirationDate = new SimpleDateFormat(javaFormat).format(expirationDate);
						attSetInstanceDescription = attSetInstanceDescription.isEmpty() ? strExpirationDate
								: attSetInstanceDescription + "_" + strExpirationDate;
					}

					attSetInst.setDescription(attSetInstanceDescription);
					OBDal.getInstance().save(attSetInst);

					OBDal.getInstance().flush();

				} finally {
					OBContext.restorePreviousMode();
				}
				return attSetInst;
			} else {
				throw new OBException("Attributes are expected for product: " + product.getIdentifier());
			}
		}
		return OBDal.getInstance().get(AttributeSetInstance.class, "0");
	}

	private static Boolean areDifferencesBetweenAttributesIncludingHeaders(JSONObject jsonTask) {
		Map<String, String> expValues = createAttributeMapBasedOnExpectedAttributesValues(jsonTask, true);
		Map<String, String> confValues = createAttributeMapBasedOnConfirmedAttributesValues(jsonTask, true);
		if (expValues.size() != confValues.size()) {
			return true;
		}
		for (String key : expValues.keySet()) {
			if (!expValues.get(key).equals(confValues.get(key))) {
				return true;
			}
		}
		return false;
	}

	private static String getAttributeValue(JSONObject attValues, Attribute att) throws JSONException {
		if (attValues.has(att.getId()) && attValues.getJSONObject(att.getId()).has("value")
				&& attValues.getJSONObject(att.getId()).getString("value") != null
				&& !attValues.getJSONObject(att.getId()).getString("value").equals("null")
				&& attValues.getJSONObject(att.getId()).getString("value").length() > 0) {
			return attValues.getJSONObject(att.getId()).getString("value");
		} else {
			return null;
		}
	}

	public static String getValidWarehouseId() {
		String warehouseId = null;
		if (OBContext.getOBContext().getWarehouse() != null) {
			warehouseId = OBContext.getOBContext().getWarehouse().getId();
		} else {
			String where = "as e where e.organization.id in (:orgList) order by searchKey asc";
			try {
				OBContext.setAdminMode(true);

				OBQuery<Warehouse> query = OBDal.getInstance().createQuery(Warehouse.class, where.toString());
				query.setMaxResult(1);
				query.setNamedParameter("orgList", OBContext.getOBContext().getOrganizationStructureProvider()
						.getNaturalTree(OBContext.getOBContext().getCurrentOrganization().getId()));
				Warehouse wh = query.uniqueResult();
				warehouseId = wh.getId();
			} finally {
				OBContext.restorePreviousMode();
			}
		}
		return warehouseId;
	}

	/**
	 * Based on the task, this utility function will decide if attribute life cycle
	 * utilities are executed or not
	 */
	public static Boolean isAttributeChangeAllowedForTask(OBAWOTask t, JSONObject jsonTask) {
		if (t.getProduct().getAttributeSet() != null) {
			String baseTaskTypeSk = t.getTaskType().getBaseTaskType().getSearchKey();
			if (baseTaskTypeSk.equals(OBAWO_Constants.BTT_ADJUSTMENT)) {
				// Adjustments never allows to change attributes
				return false;
			}
			if (baseTaskTypeSk.equals(OBAWO_Constants.BTT_MOVE)) {
				if (AWOReferencedInventoryUtils.isTaskInvolvedInReferenceInventory(t)) {
					// When a movement is done with reference inventory
					// atts cannot be changed
					return false;
				}
				if (t.getExpectedAttribute() == null) {
					// If expected attributes are null, then they can be changed
					return true;
				} else if (hasTaskAnyExpectedAttributeEmpty(jsonTask) || !AWOAttributeLifeCycleUtils
						.getUpdatableAttributeIds(t.getProduct().getAttributeSet().getId()).isEmpty()) {
					return true;
				}
			}
			if (baseTaskTypeSk.equals(OBAWO_Constants.BTT_ISSUE)) {
				return false;
			}
			if (baseTaskTypeSk.equals(OBAWO_Constants.BTT_RECEIPT)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Generates a map with the values of every attribute which appear in confirmed
	 * attribute values object
	 */
	private static Map<String, String> createAttributeMapBasedOnConfirmedAttributesValues(JSONObject jsonTask,
			Boolean includeHeaders) {
		Map<String, String> mapToReturn = new HashMap<String, String>();

		if (jsonTask.has("confirmedAttValues") && !jsonTask.isNull("confirmedAttValues")) {
			JSONObject confAttValues = null;
			try {
				confAttValues = jsonTask.getJSONObject("confirmedAttValues");
			} catch (JSONException e) {
				// Should never happen
				throw new OBException("An error happened reading confirmedAttValues from task", e, true);
			}
			if (confAttValues != null) {
				Iterator<?> keys = confAttValues.keys();
				while (keys.hasNext()) {
					String key = (String) keys.next();
					String currentAttributeValue = "";
					Boolean isHeaderAtt = false;
					if (includeHeaders == false) {
						if (key.equals("serialno") || key.equals("lot") || key.equals("guaranteedate")) {
							// These attributes are ignored because they cannot be changed
							continue;
						}
					}
					Attribute currentAttribute = null;
					if (key.equals("serialno") || key.equals("lot") || key.equals("guaranteedate")) {
						isHeaderAtt = true;
					}
					if (!isHeaderAtt) {
						currentAttribute = OBDal.getInstance().get(Attribute.class, key);
					}
					JSONObject currentJsonObjAtt;
					try {
						currentJsonObjAtt = confAttValues.getJSONObject(key);
					} catch (JSONException e1) {
						// Should never happen. Controlled before
						throw new OBException(
								"An error happened reading key " + key + " from confirmedAttValues of task", e1, true);
					}

					if (OBMOBCUtils.isJsonObjectPropertyStringPresentNotNullAndNotEmptyString(currentJsonObjAtt,
							"value")) {

						try {
							currentAttributeValue = currentJsonObjAtt.getString("value");
						} catch (JSONException e2) {
							// Should never happen. Controlled before
							throw new OBException("An error happened reading value of attribute with key " + key
									+ " from confirmedAttValues of task", e2, true);
						}
						if (!isHeaderAtt && currentAttribute != null && currentAttribute.isList()) {
							// find the value id
							OBQuery<AttributeValue> attValueQuery = OBDal.getInstance()
									.createQuery(AttributeValue.class, "attribute.id = :attId and name = :value");
							attValueQuery.setNamedParameter("value", currentAttributeValue);
							attValueQuery.setNamedParameter("attId", key);
							attValueQuery.setFetchSize(1);
							AttributeValue attValueFound = attValueQuery.uniqueResult();
							if (attValueFound != null) {
								// Attribute value is now an ID of the value
								currentAttributeValue = attValueFound.getId();
							}
						}
					}
					mapToReturn.put(key, currentAttributeValue);
				}
			}
		}
		return mapToReturn;
	}

	/**
	 * Generates a map with the values of every attribute which appear in expected
	 * attribute values object
	 */
	private static Map<String, String> createAttributeMapBasedOnExpectedAttributesValues(JSONObject jsonTask,
			Boolean includeHeaders) {
		Map<String, String> mapToReturn = new HashMap<String, String>();

		if (jsonTask.has("expectedAttValues") && !jsonTask.isNull("expectedAttValues")) {
			JSONObject expectedAttValues = null;
			try {
				expectedAttValues = jsonTask.getJSONObject("expectedAttValues");
			} catch (JSONException e) {
				// Should never happen
				throw new OBException("An error happened reading expectedAttValues from task", e, true);
			}
			if (expectedAttValues != null) {
				Iterator<?> keys = expectedAttValues.keys();
				while (keys.hasNext()) {
					Boolean isHeaderAtt = false;
					String key = (String) keys.next();
					String currentAttributeValue = "";
					if (includeHeaders == false) {
						if (key.equals("serialno") || key.equals("lot") || key.equals("guaranteedate")) {
							// These attributes are ignored because they cannot be changed
							continue;
						}
					}
					Attribute currentAttribute = null;
					if (key.equals("serialno") || key.equals("lot") || key.equals("guaranteedate")) {
						isHeaderAtt = true;
					}
					if (!isHeaderAtt) {
						currentAttribute = OBDal.getInstance().get(Attribute.class, key);
					}
					JSONObject currentJsonObjAtt;
					try {
						currentJsonObjAtt = expectedAttValues.getJSONObject(key);
					} catch (JSONException e1) {
						// Should never happen. Controlled before
						throw new OBException(
								"An error happened reading key " + key + " from expectedAttValues of task", e1, true);
					}

					if (OBMOBCUtils.isJsonObjectPropertyStringPresentNotNullAndNotEmptyString(currentJsonObjAtt,
							"value")) {

						try {
							currentAttributeValue = currentJsonObjAtt.getString("value");
						} catch (JSONException e2) {
							// Should never happen. Controlled before
							throw new OBException("An error happened reading value of attribute with key " + key
									+ " from expectedAttValues of task", e2, true);
						}
						if (!isHeaderAtt && currentAttribute != null && currentAttribute.isList()) {
							// find the value id
							OBQuery<AttributeValue> attValueQuery = OBDal.getInstance()
									.createQuery(AttributeValue.class, "attribute.id = :attId and name = :value");
							attValueQuery.setNamedParameter("value", currentAttributeValue);
							attValueQuery.setNamedParameter("attId", key);
							attValueQuery.setFetchSize(1);
							AttributeValue attValueFound = attValueQuery.uniqueResult();
							if (attValueFound != null) {
								// Attribute value is now an ID of the value
								currentAttributeValue = attValueFound.getId();
							}
						}
					}
					mapToReturn.put(key, currentAttributeValue);
				}
			}
		}
		return mapToReturn;
	}

	/**
	 * Returns true if a certain expected att of json task is null or empty
	 */
	private static Boolean hasTaskAnyExpectedAttributeEmpty(JSONObject jsonTask) {
		if (jsonTask.has("expectedAttValues") && !jsonTask.isNull("expectedAttValues")) {
			JSONObject expAttValues = null;
			try {
				expAttValues = jsonTask.getJSONObject("expectedAttValues");
			} catch (JSONException e) {
				// Should never happen
				throw new OBException("An error happened reading expectedAttValues from task", e, true);
			}
			if (expAttValues != null) {
				Iterator<?> keys = expAttValues.keys();
				boolean anyNullAtt = false;
				while (keys.hasNext()) {
					JSONObject currentJsonObjAtt = null;
					String key = (String) keys.next();
					if (key.equals("serialno") || key.equals("lot") || key.equals("guaranteedate")) {
						// These attributes are ignored because they cannot be changed
						continue;
					}
					try {
						currentJsonObjAtt = expAttValues.getJSONObject(key);
					} catch (JSONException e1) {
						// Should never happen. Controlled before
						throw new OBException(
								"An error happened reading key " + key + " from expectedAttValues of task", e1, true);
					}
					if (!OBMOBCUtils.isJsonObjectPropertyStringPresentNotNullAndNotEmptyString(currentJsonObjAtt,
							"value")) {
						anyNullAtt = true;
					}
				}
				if (anyNullAtt) {
					return true;
				}
			}
		}
		return false;
	}
}
