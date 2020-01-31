package com.doceleguas.warehouse.advancedwarehouseoperations.olb.ad_process;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.hibernate.Session;
import org.hibernate.criterion.Restrictions;
import org.hibernate.query.Query;
import org.openbravo.base.exception.OBException;
import org.openbravo.dal.core.OBContext;
import org.openbravo.dal.service.OBCriteria;
import org.openbravo.dal.service.OBDal;
import org.openbravo.dal.service.OBQuery;
import org.openbravo.mobile.core.MobileCoreConstants;
import org.openbravo.model.ad.access.User;
import org.openbravo.scheduling.ProcessBundle;
import org.openbravo.scheduling.ProcessLogger;
import org.openbravo.service.db.DalBaseProcess;
import org.openbravo.warehouse.advancedwarehouseoperations.OBAWOTask;
import org.quartz.JobExecutionException;

import com.doceleguas.warehouse.advancedwarehouseoperations.olb.data.TaskAssignConfig;
import com.precognis.awo.Incidencias;

//the background process needs to extend DalBaseProcess since
public class TaskAssignmentConfiguration extends DalBaseProcess {

	private ProcessLogger logger;
	private TaskUserAssignments availability;
	private List<String> connectedUsers;
	final Map<String, Boolean> assignedTask = new HashMap<>();

	protected void doExecute(ProcessBundle bundle) throws Exception {

		logger = bundle.getLogger();
		logger.logln("Starting Operator Load Balancing");
		try {
			UnAssignTasks();
			if (thereAreConnectedUsers()) {
				logger.logln(String.format("%d users are connected in the POS.", getConnectedUsers().size()));
				availability = new TaskUserAssignments(getConnectedUsers());
				startAssignProcess();
			} else {
				logger.logln("No users are connected");
			}
		} catch (Exception e) {
			logger.logln(String.format("ERROR: %s", e.getMessage()));
			throw new JobExecutionException(e.getMessage(), e);
		}

	}

	private void UnAssignTasks() throws Exception {

		StringBuilder queryBuilder = new StringBuilder();
		queryBuilder.append("update " + OBAWOTask.ENTITY_NAME);
		queryBuilder.append(" set " + OBAWOTask.PROPERTY_USERCONTACT + " = NULL");
		queryBuilder.append(" where " + OBAWOTask.PROPERTY_STATUS + "='AV'");

		if (!getConnectedUsers().isEmpty()) {
			queryBuilder.append(" and " + OBAWOTask.PROPERTY_USERCONTACT + ".id not in (:ids)");
		}
		queryBuilder.append(" and " + OBAWOTask.PROPERTY_USERCONTACT + " is not null ");
		final Session session = OBDal.getInstance().getSession();
		@SuppressWarnings("rawtypes")
		final Query query = session.createQuery(queryBuilder.toString());
		if (!getConnectedUsers().isEmpty()) {
			query.setParameter("ids", getConnectedUsers());
		}
		query.executeUpdate();
	}

	/**
	 * This method get all Rules ordered by rules of kind Rules to Tasks and seqNo
	 * 
	 * @return the list of TaskAssignConfig(rules) present in the db.
	 */
	private List<TaskAssignConfig> getRules() {
		final OBCriteria<TaskAssignConfig> rules = OBDal.getInstance().createCriteria(TaskAssignConfig.class);
		rules.addOrderBy(TaskAssignConfig.PROPERTY_ASSIGNLOGIC, true);
		rules.addOrderBy(TaskAssignConfig.PROPERTY_SEQUENCENUMBER, true);
		rules.addOrderBy(TaskAssignConfig.PROPERTY_WAREHOUSE, true);
		rules.setFilterOnReadableOrganization(false);
		rules.setFilterOnReadableClients(false);
		return rules.list();
	}

	/**
	 * For each rule get the list of tasks that match and call the task assignment
	 * process
	 */
	private void startAssignProcess() {
		List<TaskAssignConfig> rules = getRules();
		logger.logln("Se han encontrado " + rules.size() + " reglas");
		if (!rules.isEmpty()) {
			for (TaskAssignConfig rule : rules) {
				logger.logln(String.format("-- Rule %s --", rule.getSequenceNumber()));
				List<OBAWOTask> taskList = new OlbTaskFilter().filter(rule);
				logger.logln("-- La cantidad de tareas encontradas para esta regla son " + taskList.size());
				if (!taskList.isEmpty()) {
					executeTaskAssigment(rule, taskList);
				} else
					logger.logln("--- No matching tasks");
			}
		} else {
			logger.logln("-- No rules --");
		}
	}

	/**
	 * This method walks through the tasks in taskList param and assigns them to the
	 * users of the rule if users are not saturated and belong to the same
	 * organization
	 * 
	 * @param rule     The rule
	 * @param taskList The tasks
	 */
	private void executeTaskAssigment(TaskAssignConfig rule, List<OBAWOTask> taskList) {

		try {
			OBContext.setAdminMode();
			List<User> userFromRuleList = getUsersConnectedFromRule(rule);

			logger.logln("-- Los usuarios encontrados con esta regla son " + userFromRuleList.size() + " : ");
			for (User user : userFromRuleList) {
				logger.logln("--- " + user.getUsername());
			}

			if (userFromRuleList.isEmpty()) {
				logger.logln("--- 0 users connected");
			} else {

				List<OBAWOTask> groupOfTaskList;

				for (OBAWOTask task : taskList) {

					logger.logln("-- tratando la tarea: " + task.getIdentifier());

					if (!assignedTask.containsKey(task.getId())) {

						logger.logln("-- La tarea no ha sido tratado todavía");

						if (task.getExpectedMRefinventory() != null) {
							groupOfTaskList = task.getWarehousePickingList().getOBAWOTaskList();
						} else
							groupOfTaskList = Collections.singletonList(task);

						User u = availability.getUserWithLessAssignedTasks(userFromRuleList);
						logger.logln("-- Usuarios con menos trabajo: " + (u != null ? u.getUsername() : "no hay"));

						if (u != null) {
							assignTaskToTheUser(groupOfTaskList, u);
							markTasksAsAssigned(groupOfTaskList);
						}
					}
					logger.logln("-- Final del tratamiento de la tarea");
				}
				OBDal.getInstance().flush();
			}
		} catch (Exception e) {
			throw new OBException(e);
		} finally {
			OBContext.restorePreviousMode();
		}
	}

	private void markTasksAsAssigned(List<OBAWOTask> groupOfTaskList) {
		for (OBAWOTask task : groupOfTaskList) {
			logger.logln(String.format("--- Marcando la tarea como asignada: " + task.getIdentifier()));

			assignedTask.put(task.getId(), true);
		}
	}

	private void assignTaskToTheUser(List<OBAWOTask> taskList, User u) {

		// TODO si algún día se actualiza este módulo hay que añadir las lineas
		// asociadas a incidencias

		boolean incidencias = obtenerIncidenciasViejas(taskList);

		logger.logln(String.format("--- Incidencias encontradas: " + incidencias));

		if (!incidencias) {

			for (OBAWOTask task : taskList) {

				task.setUserContact(u);

				OBDal.getInstance().save(task);
				logger.logln(String.format("--- Assigned task with id %s to user %s", task.getId(), u.getUsername()));

			}

			availability.incrementUserAssignedTaskCount(u.getId());
		}

	}

	public static boolean obtenerIncidenciasViejas(List<OBAWOTask> taskList) {

		OBContext.setAdminMode(false);
		try {

			for (OBAWOTask task : taskList) {

				OBCriteria<Incidencias> incidenciasCriteria = OBDal.getInstance().createCriteria(Incidencias.class);
				incidenciasCriteria.setFilterOnReadableOrganization(false);
				incidenciasCriteria.add(Restrictions.eq(Incidencias.PROPERTY_TAREA, task.getId()));
				Incidencias result = (Incidencias) incidenciasCriteria.uniqueResult();

				if (result != null) {
					return true;
				}
			}
		} finally {
			OBContext.restorePreviousMode();
		}

		return false;
	}

	/**
	 * Get the users from the rule {@link TaskAssignConfig}
	 * 
	 * @param rule
	 * 
	 * @return the list of users of the rule that are connected
	 */

	private List<User> getUsersConnectedFromRule(TaskAssignConfig rule) {
		final Map<String, Object> queryParams = new HashMap<>(5);
		StringBuilder whereClause = new StringBuilder("as u where 1=1");

		if (!rule.isAllowFilterByUser() && rule.getUserContact() != null) {
			whereClause.append(" and u.id = :operator");
			queryParams.put("operator", rule.getUserContact().getId());
		} else if (rule.getPosition() != null && !StringUtils.equals(rule.getPosition(), "%")) {
			whereClause.append(" and lower(u.position) LIKE :position");
			queryParams.put("position", rule.getPosition().toLowerCase());
		}

		// TODO añadido por Opentix dado que se quiere filtrar por puesto de trabajo
		if (rule.getOpxdesPuestoTrabajo() != null) {

			if (rule.getOpxdesPuestoTrabajo().isAnularTaskAssigment()) {
				logger.logln(String.format("--- Anulando el task assigment debido al puesto de trabajo"));
				whereClause.append(" and u.opxdesPuestoTrabajo.id = :puestoDeTrabajo");
				queryParams.put("puestoDeTrabajo", "no existe");
			} else {
				whereClause.append(" and u.opxdesPuestoTrabajo = :puestoDeTrabajo");
				queryParams.put("puestoDeTrabajo", rule.getOpxdesPuestoTrabajo());
			}

		}

		whereClause.append(" and u.id IN (:ids)");
		queryParams.put("ids", getConnectedUsers());

		whereClause.append(" and u.defaultWarehouse.id = :warehouseRule");
		queryParams.put("warehouseRule", rule.getWarehouse().getId());

		final OBQuery<User> userQuery = OBDal.getInstance().createQuery(User.class, whereClause.toString());

		userQuery.setNamedParameters(queryParams);
		userQuery.setFilterOnReadableClients(false);
		userQuery.setFilterOnReadableOrganization(false);
		return userQuery.list();
	}

	/**
	 * Return the User IDs of the connected users in the Web POS. If it was already
	 * retrieved then it is returned else it is executed a query to the database
	 * 
	 * @return The User ID List of connected users to Web POS
	 */
	private List<String> getConnectedUsers() {
		if (connectedUsers != null) {
			return connectedUsers;
		}

		StringBuilder usersConnectedHql = new StringBuilder();
		usersConnectedHql.append(" select distinct u.id");
		usersConnectedHql.append(" from ADUser u,");
		usersConnectedHql.append(" ADSession s");
		usersConnectedHql.append(" where u.username=s.username");
		usersConnectedHql.append(" and s.sessionActive='Y'");
		usersConnectedHql.append(" and s.loginStatus = :webPosSessionType");

		final Session session = OBDal.getInstance().getSession();
		final Query<String> query = session.createQuery(usersConnectedHql.toString(), String.class);
		query.setParameter("webPosSessionType", MobileCoreConstants.SESSION_TYPE);
		connectedUsers = query.list();

		return connectedUsers;
	}

	/**
	 * Return if exists any user connected to the Web POS
	 * 
	 * @return True if there is any connected user to the web pos or not
	 */
	private boolean thereAreConnectedUsers() {
		return !getConnectedUsers().isEmpty();
	}
}
