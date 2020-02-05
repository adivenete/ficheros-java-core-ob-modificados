/************************************************************************************ 
 * Copyright (C) 2009-2018 Openbravo S.L.U. 
 * Licensed under the Openbravo Commercial License version 1.0 
 * You may obtain a copy of the License at http://www.openbravo.com/legal/obcl.html 
 ************************************************************************************/

package org.openbravo.module.aeat347apr.es;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.hibernate.Query;
import org.hibernate.ScrollMode;
import org.hibernate.ScrollableResults;
import org.hibernate.Session;
import org.hibernate.criterion.Expression;
import org.hibernate.criterion.Restrictions;
import org.openbravo.dal.core.OBContext;
import org.openbravo.dal.security.OrganizationStructureProvider;
import org.openbravo.dal.service.OBCriteria;
import org.openbravo.dal.service.OBDal;
import org.openbravo.dal.service.OBQuery;
import org.openbravo.model.common.businesspartner.BusinessPartner;
import org.openbravo.model.common.businesspartner.Location;
import org.openbravo.model.common.enterprise.Organization;
import org.openbravo.model.common.enterprise.OrganizationInformation;
import org.openbravo.model.common.plm.Product;
import org.openbravo.model.financialmgmt.accounting.FIN_FinancialAccountAccounting;
import org.openbravo.model.financialmgmt.calendar.Year;
import org.openbravo.model.financialmgmt.payment.DebtPayment;
import org.openbravo.model.financialmgmt.payment.FIN_FinaccTransaction;
import org.openbravo.model.financialmgmt.payment.FIN_FinancialAccount;
import org.openbravo.model.financialmgmt.payment.FIN_Payment;
import org.openbravo.model.financialmgmt.payment.FinAccPaymentMethod;
import org.openbravo.module.aeat347apr.es.model.AEAT347_DocumentType;
import org.openbravo.module.aeat347apr.es.utility.Utility347;
import org.openbravo.module.taxreportlauncher.TaxParameter;
import org.openbravo.module.taxreportlauncher.Dao.TaxReportLauncherDao;
import org.openbravo.module.taxreportlauncher.Exception.OBTL_Exception;

/**
 * This class implements the all the data access logic of the AEAT347ReportAPR
 * servlet
 * 
 * @author openbravo
 * 
 */
public class AEAT347ReportAPRDao {

	public TaxReportLauncherDao dao = new TaxReportLauncherDao();
	private final int PAYMENT = 1;
	private final int TRANSACTION = 2;
	private final int RECONCILIATION = 3;
	static Logger log4j = Logger.getLogger(AEAT347ReportAPRDao.class);

	public AEAT347ReportAPRDao() {
	}

	/**
	 * 
	 * @param strReportId report id
	 * @param value       , value2 (optional) name of the value/s (SearchKey) given
	 *                    to the tax report parameter
	 * @return String set of ids (in a where-in format)
	 */
	public String getTaxes(String strReportId, String value1, String value2) {
		try {
			OBContext.setAdminMode(true);

			final StringBuffer where = new StringBuffer();
			where.append(" as parameter where (parameter.taxReportParameter.searchKey like '" + value1 + "'");
			if (!value2.equals(""))
				where.append(" or parameter.taxReportParameter.searchKey like '" + value2 + "' ");

			where.append(" ) and parameter.taxReportParameter.taxReportGroup.taxReport.id ='" + strReportId + "' ");
			final OBQuery<TaxParameter> query = OBDal.getInstance().createQuery(TaxParameter.class, where.toString());

			String strOut = "in (";
			for (TaxParameter tp : query.list()) {
				strOut += "'" + tp.getTax().getId() + "', ";
			}
			strOut = strOut.substring(0, strOut.length() - 2) + ")";
			return (strOut.equals("in)")) ? "" : strOut;
		} finally {
			OBContext.restorePreviousMode();
		}
	}

	/**
	 * 
	 * @param strReport ID of the report for which document types will be retrieved
	 * @return set of ids (in a where-in format)
	 */
	public String getDocTypes(String strReport) {
		try {
			OBContext.setAdminMode(true);
			String strOut = "in (";
			final OBCriteria<AEAT347_DocumentType> criteria = OBDal.getInstance()
					.createCriteria(AEAT347_DocumentType.class);
			criteria.add(Expression.eq("obtlTaxReport.id", strReport));
			for (AEAT347_DocumentType type : criteria.list()) {
				strOut += "'" + type.getDocumentType().getId() + "', ";
			}
			strOut = strOut.substring(0, strOut.length() - 2) + ")";
			return strOut;
		} finally {
			OBContext.restorePreviousMode();
		}
	}

	/**
	 * 
	 * @param strTaxesInStatement1 main string for relation of taxes id
	 * @param strTaxesInStatement2 optional string for relation of taxes id
	 * @param strDocTypes          relation of doc type ids that should be reported
	 * @param strProductFiltering  whether to filter products by those of lease type
	 *                             or not
	 * @param mapDateRange         map with initial date and end date (+1 day) of
	 *                             the year
	 * @param strAcctSchemaId      accounting schema id
	 * @param strOrgId             organization id
	 * @return List of objects array (BigDecimal, BigDecimal, Float, String) with
	 *         all business partners and amounts information: [0]: debit taxable
	 *         amount [1]: credit taxable amount [2]: business partner id [3]:
	 *         product id or empty string [4]: rate of the tax applied [5]: one
	 *         fact_acct_id of the lines involved in the entry
	 */
	@SuppressWarnings("unchecked")
	public List<Object[]> getAmounts(String strTaxesInStatement1, String strTaxesInStatement2, String strDocTypes,
			String strProductFiltering, Map<String, Date> mapDateRange, String strOrgId, String strAcctSchemaId) {
		try {
			OBContext.setAdminMode(true);

			final Organization org = dao.getOrg(strOrgId);
			OrganizationStructureProvider osp = OBContext.getOBContext()
					.getOrganizationStructureProvider(org.getClient().getId());
			final String inClauseOrgs = Utility347.createINClauseForOrgs(osp.getChildTree(org.getId(), true));

			// With this query, amounts for each entry are calculated. Using the filter of
			// documentType
			// and
			// taxId, only those operations that must go into the file will be taken into
			// account.
			final StringBuffer sql = new StringBuffer();
			String strWhereClause = "";
			if (strProductFiltering.equals("Lease")) {
				strWhereClause = getLeaseProducts(strOrgId);
				if (strWhereClause.equals(""))
					return new ArrayList<Object[]>();
			}

			sql.append(" select sum(fa.debit) as debit, sum(fa.credit) as credit");
			sql.append(", fa.businessPartner.id as businessPartner");
			if (strWhereClause.equals(""))
				sql.append(", '' as product");
			else
				sql.append(", fa.product.id as product");
			sql.append(", fa.tax.rate as rate");
			sql.append(", min(fa.id) as factAcctId");
			sql.append(" from FinancialMgmtAccountingFact as fa ");
			sql.append(" where fa.client " + OBDal.getInstance().getReadableClientsInClause());
			sql.append(" and fa.organization in (" + inClauseOrgs + ") ");
			sql.append(" and fa.active='Y'");
			sql.append(" and fa.accountingDate >= :startingDate");
			sql.append(" and fa.accountingDate < :endingDate");
			sql.append(" and fa.accountingSchema.id = '" + strAcctSchemaId + "' ");
			if (strDocTypes.length() > 3)
				sql.append(" and fa.documentType " + strDocTypes);
			sql.append(" and fa.table not in ('318', '145')");
			sql.append(" and fa.type not in ('O', 'C','R')");
			sql.append(" and fa.businessPartner.id is not null ");
			sql.append(" and (fa.tax " + strTaxesInStatement1);
			if (strTaxesInStatement2.equals(""))
				sql.append(")");
			else
				sql.append("or fa.tax " + strTaxesInStatement2 + ")");
			if (!strWhereClause.equals(""))
				sql.append(" and fa.product " + getLeaseProducts(strOrgId));
			if (strProductFiltering.equals("")) {
				sql.append(" group by fa.businessPartner, fa.tax.rate");
				sql.append(" order by fa.businessPartner");
			} else {
				sql.append(" group by fa.businessPartner, fa.product.id, fa.tax.rate");
				sql.append(" order by fa.businessPartner, fa.product.id");
			}

			final Session session = OBDal.getInstance().getSession();
			final Query query = session.createQuery(sql.toString());

			query.setParameter("startingDate", mapDateRange.get("startingDate"));
			query.setParameter("endingDate", mapDateRange.get("endingDate"));
			List lOut = query.list();

			return lOut;
		} finally {
			OBContext.restorePreviousMode();
		}
	}

	@SuppressWarnings("unchecked")
	public List<Object[]> getInvoiceAmounts(String strTaxesInStatement1, String strTaxesInStatement2,
			String strDocTypes, String strProductFiltering, Map<String, Date> mapDateRange, String strOrgId,
			String strAcctSchemaId) {
		try {
			OBContext.setAdminMode(true);

			final Organization org = dao.getOrg(strOrgId);
			OrganizationStructureProvider osp = OBContext.getOBContext()
					.getOrganizationStructureProvider(org.getClient().getId());
			final String inClauseOrgs = Utility347.createINClauseForOrgs(osp.getChildTree(org.getId(), true));

			// Amounts for each entry due to invoices are calculated. Using the filter of
			// documentType and
			// taxId, only those operations that must go into the file will be taken into
			// account.
			final StringBuffer sql = new StringBuffer();

			sql.append(" select sum(to_number(AEAT347_currency_convert(it.taxAmount, it.invoice.currency.id, '"
					+ AEAT347ReportAPR.CURRENCY_EURO_ID + "' , i.accountingDate, NULL, '" + org.getClient().getId()
					+ "', NULL, it.invoice.id))),(CASE  WHEN it.recalculate='Y' THEN (min(to_number(AEAT347_currency_convert(it.taxableAmount, it.invoice.currency.id,'"
					+ AEAT347ReportAPR.CURRENCY_EURO_ID + "' , i.accountingDate, NULL, '" + org.getClient().getId()
					+ "', NULL, it.invoice.id)))) ELSE 0 END)");
			sql.append(", i.businessPartner.id as businessPartner");
			sql.append(", '' as product");
			sql.append(", dt.documentCategory as document, dt.reversal as reversal ");
			sql.append(", coalesce(pt.id, t.id) as taxId,  i.id as invoiceId ");
			sql.append(" from InvoiceTax as it inner join it.tax as t");
			sql.append(" inner join it.invoice as i ");
			sql.append(" inner join i.documentType as dt ");
			sql.append(" left join t.parentTaxRate as pt ");
			sql.append(" where exists (select 1 ");
			sql.append("               from FinancialMgmtAccountingFact as fa ");
			sql.append("               where i.id = fa.recordID ");
			sql.append("               and fa.accountingSchema.id = '" + strAcctSchemaId + "' ");
			sql.append("               and fa.active='Y'");
			sql.append("               and fa.table = '318'");
			if (strDocTypes.length() > 3) {
				sql.append("             and fa.documentType " + strDocTypes);
			}
			sql.append("              ) ");

			sql.append(" and (t.id " + strTaxesInStatement1);
			if (strTaxesInStatement2.equals("")) {
				sql.append("   )");
			} else {
				sql.append("or t.id " + strTaxesInStatement2 + ")");
			}

			sql.append(" and i.client.id " + OBDal.getInstance().getReadableClientsInClause());
			sql.append(" and i.organization.id in (" + inClauseOrgs + ") ");
			sql.append(" and i.accountingDate >= :startingDate");
			sql.append(" and i.accountingDate < :endingDate");

			sql.append(
					" group by i.businessPartner, i.id, coalesce(pt.id, t.id), dt.documentCategory, dt.reversal, it.recalculate");
			sql.append(" order by i.businessPartner");

			final Session session = OBDal.getInstance().getSession();
			Query query = session.createQuery(sql.toString());

			query.setParameter("startingDate", mapDateRange.get("startingDate"));
			query.setParameter("endingDate", mapDateRange.get("endingDate"));

			return query.list();
		} finally {
			OBContext.restorePreviousMode();
		}
	}

	public ScrollableResults getLeaseAmounts(String strTaxesInStatement1, String strTaxesInStatement2,
			String strDocTypes, String strProductFiltering, Map<String, Date> mapDateRange, String strOrgId,
			String strAcctSchemaId) {
		try {
			OBContext.setAdminMode(true);

			final Organization org = dao.getOrg(strOrgId);
			OrganizationStructureProvider osp = OBContext.getOBContext()
					.getOrganizationStructureProvider(org.getClient().getId());
			final String inClauseOrgsChild = Utility347.createINClauseForOrgs(osp.getChildTree(org.getId(), true));

			// All invoice lines with product associated of type lease business, and that
			// have been posted
			// in the period, are retrieved

			final StringBuffer sqlInv = new StringBuffer();
			sqlInv.append("select il.id ");
			sqlInv.append("from InvoiceLine il ");
			sqlInv.append("inner join il.invoice as i ");
			sqlInv.append("inner join il.product as p ");
			sqlInv.append("inner join il.tax as t ");

			sqlInv.append("where exists (select 1 ");
			sqlInv.append(" from FinancialMgmtAccountingFact fa");
			sqlInv.append(" where fa.accountingDate >= :startingDate");
			sqlInv.append(" and fa.accountingDate < :endingDate");
			sqlInv.append(" and table='318'");
			sqlInv.append(" and i.id = fa.recordID ");
			sqlInv.append(" and fa.accountingSchema.id = '" + strAcctSchemaId + "' ");
			sqlInv.append(" and fa.organization in (" + inClauseOrgsChild + ") ");
			sqlInv.append(" ) ");

			sqlInv.append(" and p.aeat347Isleasebusiness = 'Y'");
			sqlInv.append(" and p.client.id = '" + org.getClient().getId() + "' ");
			final String inClauseOrgsParents = Utility347.createINClauseForOrgs(osp.getParentTree(org.getId(), false));
			sqlInv.append(" and p.organization.id in (" + inClauseOrgsChild + ", " + inClauseOrgsParents + ") ");

			sqlInv.append("and (t.id " + strTaxesInStatement1);
			if (strTaxesInStatement2.equals("")) {
				sqlInv.append(")");
			} else {
				sqlInv.append("or t.id " + strTaxesInStatement2 + ")");
			}

			final Session session = OBDal.getInstance().getSession();
			final Query queryInv = session.createQuery(sqlInv.toString());
			queryInv.setParameter("startingDate", mapDateRange.get("startingDate"));
			queryInv.setParameter("endingDate", mapDateRange.get("endingDate"));

			queryInv.scroll(ScrollMode.FORWARD_ONLY);
			return queryInv.scroll();
		} finally {
			OBContext.restorePreviousMode();
		}
	}

	/**
	 * Functions that retrieves the relation of business partners and amount
	 * collected by cash (if higher than a done limit)
	 * 
	 * @param strCashAmtLimit String with the amount limit multiplied by 100
	 * @param strAcctSchemaId accounting schema id
	 * @param strOrgId        organization id
	 * @return list with the mapping business partner id-amount collected in cash
	 */
	@SuppressWarnings("unchecked")
	public List<Object[]> cashAmounts(String strCashAmtLimit, Map<String, Date> mapDateRange, String strOrgId,
			String strAcctSchemaId) {
		try {
			OBContext.setAdminMode(true);

			final Organization org = dao.getOrg(strOrgId);
			OrganizationStructureProvider osp = OBContext.getOBContext()
					.getOrganizationStructureProvider(org.getClient().getId());
			final String inClauseOrgs = Utility347.createINClauseForOrgs(osp.getChildTree(org.getId(), true));

			BigDecimal bdLimit = new BigDecimal(strCashAmtLimit).divide(new BigDecimal(100), 12,
					BigDecimal.ROUND_HALF_EVEN);

			final StringBuffer sqlFA = new StringBuffer();
			sqlFA.append("select distinct recordID2");
			sqlFA.append(" from FinancialMgmtAccountingFact fa");
			sqlFA.append(" where fa.accountingDate >= :startingDate");
			sqlFA.append(" and fa.accountingDate < :endingDate");
			sqlFA.append(" and fa.client " + OBDal.getInstance().getReadableClientsInClause());
			sqlFA.append(" and fa.organization in (" + inClauseOrgs + ") ");
			sqlFA.append(" and fa.active='Y'");
			sqlFA.append(" and fa.table='407'");
			sqlFA.append(" and fa.recordID2 is not null");
			sqlFA.append(" and fa.accountingSchema.id = '" + strAcctSchemaId + "' ");

			final Session session = OBDal.getInstance().getSession();
			final Query queryFA = session.createQuery(sqlFA.toString());
			queryFA.setParameter("startingDate", mapDateRange.get("startingDate"));
			queryFA.setParameter("endingDate", mapDateRange.get("endingDate"));

			final StringBuffer sqlDP = new StringBuffer();
			sqlDP.append(
					"select dp.businessPartner.id, sum(to_number(AEAT347_currency_convert(dp.amount, dp.currency.id, '"
							+ AEAT347ReportAPR.CURRENCY_EURO_ID + "' , fa.accountingDate, NULL, '"
							+ org.getClient().getId() + "' , it.invoice)) as amount");
			sqlDP.append(" from FinancialMgmtDebtPayment dp, FinancialMgmtAccountingFact fa");
			sqlDP.append(" where dp.client " + OBDal.getInstance().getReadableClientsInClause());
			sqlDP.append(" and fa.recordID2 = dp.id ");
			sqlDP.append(" and dp.organization in (" + inClauseOrgs + ") ");
			sqlDP.append(" and dp.active='Y'");
			sqlDP.append(" and dp.id in (''");
			for (Object o : queryFA.list()) {
				sqlDP.append(", '" + (String) o + "'");
			}
			sqlDP.append(")");
			sqlDP.append(" and dp.receipt = 'Y'");
			sqlDP.append(" and fa.table='407'");
			sqlDP.append(" and fa.active='Y' ");
			sqlDP.append(" and dp.active='Y' ");
			sqlDP.append(" and fa.accountingSchema.id = '" + strAcctSchemaId + "' ");
			sqlDP.append(" group by dp.businessPartner.id");
			sqlDP.append(" having sum(to_number(AEAT347_currency_convert(dp.amount, dp.currency.id, '"
					+ AEAT347ReportAPR.CURRENCY_EURO_ID + "' , fa.accountingDate, NULL, '" + org.getClient().getId()
					+ "', it.invoice))) > " + bdLimit.toString());

			final Query queryDP = session.createQuery(sqlDP.toString());

			return queryDP.list();
		} finally {
			OBContext.restorePreviousMode();
		}
	}

	/**
	 * 
	 * @return String in a "in" format, to include in a where clause, with IDs of
	 *         products with Lease Business characteristic
	 */
	private String getLeaseProducts(final String strOrgId) {
		try {
			OBContext.setAdminMode(true);
			final Organization org = dao.getOrg(strOrgId);
			OrganizationStructureProvider osp = OBContext.getOBContext()
					.getOrganizationStructureProvider(org.getClient().getId());
			final String inClauseOrgsChild = Utility347.createINClauseForOrgs(osp.getChildTree(org.getId(), true));

			String strOut = "in (";
			final StringBuffer where = new StringBuffer();
			where.append(" as p ");
			where.append(" where p.aeat347Isleasebusiness = 'Y' ");

			final String inClauseOrgsParents = Utility347.createINClauseForOrgs(osp.getParentTree(org.getId(), false));
			where.append(" and p.organization.id in (" + inClauseOrgsChild + ", " + inClauseOrgsParents + ") ");

			final OBQuery<Product> query = OBDal.getInstance().createQuery(Product.class, where.toString());
			for (Product type : query.list()) {
				strOut += "'" + type.getId() + "', ";
			}
			strOut = strOut.substring(0, strOut.length() - 2) + ")";
			return (strOut.equals("in)")) ? "" : strOut;
		} finally {
			OBContext.restorePreviousMode();
		}
	}

	/**
	 * This functions returns the year name with the id given
	 * 
	 * @param strYearId id of the year in the c_year table
	 * @return string with the year
	 */
	public String getYear(String strYearId) {
		try {
			OBContext.setAdminMode(true);
			final Year year = OBDal.getInstance().get(Year.class, strYearId);

			if (year == null) {
				return "";
			}

			// At this point we know that all periods belong to the same year
			final Calendar cal = Calendar.getInstance();
			cal.setTime(year.getFinancialMgmtPeriodList().get(0).getStartingDate());
			return Integer.toString(cal.get(Calendar.YEAR));
		} finally {
			OBContext.restorePreviousMode();
		}
	}

	/**
	 * 
	 * @param strOrgId ad_org_id of the organization to seek
	 * @return taxId of the organization found
	 */
	public String getOrgTaxId(String strOrgId) {
		try {
			OBContext.setAdminMode(true);
			final OBCriteria<OrganizationInformation> orgInfo = OBDal.getInstance()
					.createCriteria(OrganizationInformation.class);
			orgInfo.add(Expression.eq("id", strOrgId));
			for (OrganizationInformation oi : orgInfo.list())
				return oi.getTaxID();
			return "";
		} finally {
			OBContext.restorePreviousMode();
		}
	}

	/**
	 * 
	 * @param strOrgId ad_org_id of the organization to look for
	 * @return name of the organization
	 */
	public String getOrgName(String strOrgId) {
		try {
			OBContext.setAdminMode(true);
			final Organization o = OBDal.getInstance().get(Organization.class, strOrgId);
			return ("".equals(o.getSocialName()) || o.getSocialName() == null) ? o.getName() : o.getSocialName();
		} finally {
			OBContext.restorePreviousMode();
		}
	}

	/**
	 * 
	 * @param strBPId ID of the BP wich information will be retrieved from database
	 * @return
	 */
	public BusinessPartner getBPInfo(String strBPId) {
		try {
			OBContext.setAdminMode(true);
			final OBCriteria<BusinessPartner> bp = OBDal.getInstance().createCriteria(BusinessPartner.class);
			bp.add(Expression.eq("id", strBPId));
			bp.setFilterOnActive(false);
			return bp.list().get(0);
		} finally {
			OBContext.restorePreviousMode();
		}
	}

	/**
	 * 
	 * @param lstBPLocation list of locations associated to a BP
	 * @return Location suitable to add in the report
	 */
	public org.openbravo.model.common.geography.Location getBPLocation(
			List<org.openbravo.model.common.businesspartner.Location> lstBPLocation) {
		try {
			OBContext.setAdminMode(true);
			Location l = lstBPLocation.get(0);
			for (Location loc : lstBPLocation) {
				if (loc.isTaxLocation())
					l = loc;
			}
			return l.getLocationAddress();
		} finally {
			OBContext.restorePreviousMode();
		}
	}

	/**
	 * Returns the information of the product with given ID
	 * 
	 * @param strProductId Id of the product which information will be retrieved
	 * @return
	 */
	public Product getProductInfo(String strProductId) {
		try {
			OBContext.setAdminMode(true);
			OBCriteria<Product> prod = OBDal.getInstance().createCriteria(Product.class);
			prod.add(Expression.eq("id", strProductId));
			prod.setFilterOnActive(false);
			return prod.list().get(0);
		} finally {
			OBContext.restorePreviousMode();
		}
	}

	/*
	 * Return a Map with the starting and ending date (+1) of the given year
	 * 
	 * @param string with the c_year_id
	 * 
	 * @return a Map with the starting and ending date of a period's list, where:
	 * <ul> <li>"startingDate" is the key for the starting day</li> <li>"endingDate"
	 * is the key for the ending day</li> </ul>
	 */
	@Deprecated
	public Map<String, Date> getDateRange(String strYearId) {
		try {
			return getDateRange(strYearId, 0);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return new HashMap<String, Date>(1);
		}
	}

	/**
	 * Return a Map with the starting and ending date (+1) of the given year or
	 * quarter of that year
	 * 
	 * @param strYearId c_year_id for which the date ranges must be returned
	 * @param iQuarter  0: whole year; 1 to 4: 1st to 4th quarter of the year; other
	 *                  case: error
	 * @return
	 * @throws ParseException
	 */
	public Map<String, Date> getDateRange(String strYearId, int iQuarter) throws ParseException {
		try {
			OBContext.setAdminMode(true);

			Map<String, Date> datesMap = new HashMap<String, Date>(1);
			String strYear = getYear(strYearId);
			SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy-MM-dd");
			Date initialDate, finalDate;
			switch (iQuarter) {
			case 0:
				initialDate = outputFormat.parse(strYear + "-01-01");
				finalDate = outputFormat.parse(strYear + "-12-31");
				break;
			case 1:
				initialDate = outputFormat.parse(strYear + "-01-01");
				finalDate = outputFormat.parse(strYear + "-03-31");
				break;
			case 2:
				initialDate = outputFormat.parse(strYear + "-04-01");
				finalDate = outputFormat.parse(strYear + "-06-30");
				break;
			case 3:
				initialDate = outputFormat.parse(strYear + "-07-01");
				finalDate = outputFormat.parse(strYear + "-09-30");
				break;
			case 4:
				initialDate = outputFormat.parse(strYear + "-10-01");
				finalDate = outputFormat.parse(strYear + "-12-31");
				break;
			default:
				return datesMap;
			}

			datesMap.put("startingDate", initialDate);
			Calendar endCal = Calendar.getInstance();
			endCal.setTime(finalDate);
			endCal.add(Calendar.DATE, 1);
			datesMap.put("endingDate", endCal.getTime());

			return datesMap;
		} finally {
			OBContext.restorePreviousMode();
		}
	}

	@Deprecated
	public List<DebtPayment> getCashPayments(String strCashAmtLimit, Map<String, Date> mapDateRange, String strOrgId,
			String strAcctSchemaId) {
		return getCashPayments(mapDateRange, strOrgId, strAcctSchemaId);
	}

	public List<DebtPayment> getCashPayments(Map<String, Date> mapDateRange, String strOrgId, String strAcctSchemaId) {
		try {
			OBContext.setAdminMode(true);

			final Organization org = dao.getOrg(strOrgId);
			OrganizationStructureProvider osp = OBContext.getOBContext()
					.getOrganizationStructureProvider(org.getClient().getId());
			final String inClauseOrgs = Utility347.createINClauseForOrgs(osp.getChildTree(org.getId(), true));

			final StringBuffer sqlFA = new StringBuffer();
			sqlFA.append(" as dp ");
			sqlFA.append(" where dp.receipt = 'Y' ");
			sqlFA.append(" and exists( select 1 from FinancialMgmtAccountingFact fa ");
			sqlFA.append(" where fa.recordID2 = dp.id ");
			sqlFA.append(" and fa.accountingDate >= :startingDate");
			sqlFA.append(" and fa.accountingDate < :endingDate");
			sqlFA.append(" and fa.client " + OBDal.getInstance().getReadableClientsInClause());
			sqlFA.append(" and fa.organization in (" + inClauseOrgs + ") ");
			sqlFA.append(" and fa.active='Y'");
			sqlFA.append(" and fa.table='407'");
			sqlFA.append(" and fa.accountingSchema.id = '" + strAcctSchemaId + "') ");
			sqlFA.append(" and exists ( select 1 from FinancialMgmtJournalLine cjl ");
			sqlFA.append(" where dp.id = cjl.payment.id ");
			sqlFA.append(" and cjl.cashJournal.posted = 'Y' ) ");

			final OBQuery<DebtPayment> queryFA = OBDal.getInstance().createQuery(DebtPayment.class, sqlFA.toString());
//			List<Object> params = new ArrayList<Object>();
//			params.add(mapDateRange.get("startingDate"));
//			params.add(mapDateRange.get("endingDate"));

			queryFA.setNamedParameter("startingDate", mapDateRange.get("startingDate"));
			queryFA.setNamedParameter("endingDate", mapDateRange.get("endingDate"));

//			queryFA.setParameters(params);

			return Collections.unmodifiableList(queryFA.list());
		} finally {
			OBContext.restorePreviousMode();
		}
	}

	public List<FIN_FinaccTransaction> getAPRCashTransactions(Map<String, Date> dateRange, String strOrgId) {
		OBContext.setAdminMode(false);
		try {
			// Let's retrieve all the transaction that...
			OBCriteria<FIN_FinaccTransaction> criTrans = OBDal.getInstance()
					.createCriteria(FIN_FinaccTransaction.class);
			// 1) Are created for financial accounts of type "Cash"
			criTrans.createAlias(FIN_FinaccTransaction.PROPERTY_ACCOUNT, "a", OBCriteria.LEFT_JOIN);

			criTrans.add(Restrictions.eq("a." + FIN_FinancialAccount.PROPERTY_TYPE, "C"));
			criTrans.createAlias(FIN_FinaccTransaction.PROPERTY_FINPAYMENT, "p", OBCriteria.LEFT_JOIN);
			// 2) It's a payment, not a withdrawal
			criTrans.add(Restrictions.or(Restrictions.ne(FIN_FinaccTransaction.PROPERTY_DEPOSITAMOUNT, BigDecimal.ZERO),
					Restrictions.eq("p." + FIN_Payment.PROPERTY_RECEIPT, true)));// eca
			// 3) The transaction took place between the two provided dates (according to
			// posting date, to
			// be coherent with getCashPayments function
			criTrans.add(Restrictions.ge(FIN_FinaccTransaction.PROPERTY_DATEACCT, dateRange.get("startingDate")));
			criTrans.add(Restrictions.lt(FIN_FinaccTransaction.PROPERTY_DATEACCT, dateRange.get("endingDate")));
			// 4) And for the corresponding organizations
			criTrans.add(Restrictions.in(FIN_FinaccTransaction.PROPERTY_ORGANIZATION + ".id",
					OBContext.getOBContext().getOrganizationStructureProvider().getNaturalTree(strOrgId)));

			// 5) Transaction (or another step of the APR flow) is posted.
			List<FIN_FinaccTransaction> lResult = removeNonPostedTransactions(criTrans.list(), strOrgId);
			// criTrans.createAlias(FIN_FinaccTransaction.PROPERTY_RECONCILIATION, "r");
			// LogicalExpression reconciliationPosted = Restrictions.and(Restrictions.eq(
			// FIN_FinaccTransaction.PROPERTY_POSTED, "D"), Restrictions.eq("r."
			// + FIN_Reconciliation.PROPERTY_POSTED, "Y"));
			// criTrans.add(Restrictions.or(Restrictions.eq(FIN_FinaccTransaction.PROPERTY_POSTED,
			// "Y"),
			// reconciliationPosted));
			return Collections.unmodifiableList(lResult);
		} finally {
			OBContext.restorePreviousMode();
		}

	}

	public List<String> getAPRCashTransactionIds(Map<String, Date> dateRange, String strOrgId) {
		final StringBuilder hqlString = new StringBuilder();
		hqlString.append("select ft.id");
		hqlString.append(" from FIN_Finacc_Transaction as ft");
		hqlString.append(" left outer join ft.account as fa");
		hqlString.append(" where fa.type = 'C'");
		hqlString.append(" and ft.depositAmount <> 0");
		hqlString.append(" and ft.dateAcct >= :startingDate");
		hqlString.append(" and ft.dateAcct < :endingDate");
		dateRange.get("endingDate");
		hqlString.append(" and ft.organization.id in :organizations");
		final Session session = OBDal.getInstance().getSession();
		final Query query = session.createQuery(hqlString.toString());
		query.setParameter("startingDate", dateRange.get("startingDate"));
		query.setParameter("endingDate", dateRange.get("endingDate"));
		query.setParameterList("organizations",
				OBContext.getOBContext().getOrganizationStructureProvider().getNaturalTree(strOrgId));
		List<String> result = new ArrayList<String>();
		for (Object resultObject : query.list()) {
			if (resultObject instanceof String) {
				result.add((String) resultObject);
			}
		}
		return result;
	}

	/**
	 * Given a set of transactions, filters those that have not been posted (taking
	 * into account the possible configurations of the accounting in APR)
	 */
	private List<FIN_FinaccTransaction> removeNonPostedTransactions(List<FIN_FinaccTransaction> list, String strOrgId) {
		ArrayList<FIN_FinaccTransaction> lResult = new ArrayList<FIN_FinaccTransaction>();
		for (FIN_FinaccTransaction trx : list) {
			if ("Y".equals(trx.getPosted())) {
				lResult.add(trx);
				continue;
			}
			switch (levelWhereTrxArePosted(trx)) {
			case TRANSACTION:
				break;
			case RECONCILIATION:
				if (trx.getReconciliation() != null && "Y".equals(trx.getReconciliation().getPosted())) {
					lResult.add(trx);
				}
				break;
			case PAYMENT:
				if (trx.getFinPayment() != null && "Y".equals(trx.getFinPayment().getPosted())) {
					lResult.add(trx);
				}
				break;
			default:
				log4j.error("Couldn't stablish the posting status of the transaction " + trx.getIdentifier() + ".");
				throw new OBTL_Exception("@Error@",
						"@AEAT347_PostingStatusNotDeterminedForTransaction@" + trx.getIdentifier());
			}
		}
		return lResult;
	}

	/**
	 * Determines, given the APR configuration, at which level does the posting take
	 * place for transactions.
	 */
	int levelWhereTrxArePosted(FIN_FinaccTransaction transaction) {
		OBContext.setAdminMode();
		try {
			List<FIN_FinancialAccountAccounting> accounts = transaction.getAccount().getFINFinancialAccountAcctList();
			FIN_Payment payment = transaction.getFinPayment();
			FinAccPaymentMethod paymentMethod = null;
			if (payment != null) {
				OBCriteria<FinAccPaymentMethod> obCriteria = OBDal.getInstance()
						.createCriteria(FinAccPaymentMethod.class);
				obCriteria.add(Restrictions.eq(FinAccPaymentMethod.PROPERTY_ACCOUNT, payment.getAccount()));
				obCriteria.add(Restrictions.eq(FinAccPaymentMethod.PROPERTY_PAYMENTMETHOD, payment.getPaymentMethod()));
				obCriteria.setFilterOnReadableClients(false);
				obCriteria.setFilterOnReadableOrganization(false);
				List<FinAccPaymentMethod> paymentMethods = obCriteria.list();
				if (paymentMethods.size() > 0) {
					paymentMethod = paymentMethods.get(0);
					for (FIN_FinancialAccountAccounting account : accounts) {
						if (payment.isReceipt()) {
							if (("INT").equals(paymentMethod.getUponDepositUse())
									&& account.getInTransitPaymentAccountIN() != null)
								return TRANSACTION;
							else if (("DEP").equals(paymentMethod.getUponDepositUse())
									&& account.getDepositAccount() != null)
								return TRANSACTION;
							else if (("CLE").equals(paymentMethod.getUponDepositUse())
									&& account.getClearedPaymentAccount() != null)
								return TRANSACTION;
							if (("INT").equals(paymentMethod.getINUponClearingUse())
									&& account.getInTransitPaymentAccountIN() != null)
								return RECONCILIATION;
							else if (("DEP").equals(paymentMethod.getINUponClearingUse())
									&& account.getDepositAccount() != null)
								return RECONCILIATION;
							else if (("CLE").equals(paymentMethod.getINUponClearingUse())
									&& account.getClearedPaymentAccount() != null)
								return RECONCILIATION;
						} else {
							if (("INT").equals(paymentMethod.getUponWithdrawalUse())
									&& account.getFINOutIntransitAcct() != null)
								return TRANSACTION;
							else if (("WIT").equals(paymentMethod.getUponWithdrawalUse())
									&& account.getWithdrawalAccount() != null)
								return TRANSACTION;
							else if (("CLE").equals(paymentMethod.getUponWithdrawalUse())
									&& account.getClearedPaymentAccountOUT() != null)
								return TRANSACTION;
							if (("INT").equals(paymentMethod.getOUTUponClearingUse())
									&& account.getFINOutIntransitAcct() != null)
								return RECONCILIATION;
							else if (("WIT").equals(paymentMethod.getOUTUponClearingUse())
									&& account.getWithdrawalAccount() != null)
								return RECONCILIATION;
							else if (("CLE").equals(paymentMethod.getOUTUponClearingUse())
									&& account.getClearedPaymentAccountOUT() != null)
								return RECONCILIATION;
						}
					}
				}
			}
			for (FIN_FinancialAccountAccounting account : accounts) {
				if (("BPD".equals(transaction.getTransactionType()) && account.getDepositAccount() != null)
						|| ("BPW".equals(transaction.getTransactionType()) && account.getWithdrawalAccount() != null)
						|| ("BF".equals(transaction.getTransactionType()) && account.getWithdrawalAccount() != null))
					return TRANSACTION;
			}
			if (transaction.getGLItem() != null) {
				for (FIN_FinancialAccountAccounting account : accounts) {
					if ("BPD".equals(transaction.getTransactionType()) && account.getClearedPaymentAccount() != null) {
						return RECONCILIATION;
					} else if ("BPW".equals(transaction.getTransactionType())
							&& account.getClearedPaymentAccountOUT() != null) {
						return RECONCILIATION;
					}
				}
			}
			for (FIN_FinancialAccountAccounting account : accounts) {
				if ("BF".equals(transaction.getTransactionType()) && account.getClearedPaymentAccountOUT() != null) {
					return RECONCILIATION;
				}
			}
			if (payment != null && paymentMethod != null
					&& ("RPR".equals(payment.getStatus()) || "PPM".equals(payment.getStatus())
							|| "RDNC".equals(payment.getStatus()) || "PWNC".equals(payment.getStatus())
							|| "RPPC".equals(payment.getStatus()))) {
				for (FIN_FinancialAccountAccounting account : accounts) {
					if (payment.isReceipt()) {
						if (("INT").equals(paymentMethod.getUponReceiptUse())
								&& account.getInTransitPaymentAccountIN() != null)
							return PAYMENT;
						else if (("DEP").equals(paymentMethod.getUponReceiptUse())
								&& account.getDepositAccount() != null)
							return PAYMENT;
						else if (("CLE").equals(paymentMethod.getUponReceiptUse())
								&& account.getClearedPaymentAccount() != null)
							return PAYMENT;
					} else {
						if (("INT").equals(paymentMethod.getUponPaymentUse())
								&& account.getFINOutIntransitAcct() != null)
							return PAYMENT;
						else if (("WIT").equals(paymentMethod.getUponPaymentUse())
								&& account.getWithdrawalAccount() != null)
							return PAYMENT;
						else if (("CLE").equals(paymentMethod.getUponPaymentUse())
								&& account.getClearedPaymentAccountOUT() != null)
							return PAYMENT;
					}
					if (payment.getAmount().compareTo(BigDecimal.ZERO) == 0) {
						return PAYMENT;
					}
				}
			}
			return -1;
		} catch (Exception e) {
			log4j.error("ERROR: " + e);
			return -1;
		} finally {
			OBContext.restorePreviousMode();
		}
	}

	/**
	 * Returns the debt payment that corresponds to the provided payment. In case it
	 * doesn't exist, returns null
	 */
	public DebtPayment getMigratedPayment(FIN_Payment payment) {
		final Class<DebtPayment> debtPaymentClass = DebtPayment.class;
		try {
			OBContext.setAdminMode(true);
			debtPaymentClass.getMethod("getAPRMTPayment", new Class[0]);

			final StringBuffer hql = new StringBuffer();
			hql.append(" as dp ");
			hql.append(" where dp.aPRMTPayment.id = '" + payment.getId() + "'");

			final OBQuery<DebtPayment> query = OBDal.getInstance().createQuery(DebtPayment.class, hql.toString());
			List<DebtPayment> l = query.list();
			if (l.size() > 0) {
				return l.get(0);
			} else {
				return null;
			}
		} catch (final Exception e) {
			return null;
		} finally {
			OBContext.restorePreviousMode();
		}
	}

}
