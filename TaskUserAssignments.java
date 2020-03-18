package com.doceleguas.warehouse.advancedwarehouseoperations.olb.ad_process;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hibernate.query.Query;
import org.openbravo.dal.service.OBDal;
import org.openbravo.model.ad.access.User;

/**
 * Handle limit of task and assigned task for all connected users in the POS
 * 
 * @author alain.perez@doceleguas.com
 */

public class TaskUserAssignments {
	// This map contains the max of tasks can be assigned to an user. The map key
	// contains the User ID
	// and the value is the limit of tasks
	private final Map<String, Integer> userLimitOfTasks = new HashMap<>();
	// This map contains the already assigned task count of an user. The map key
	// contains the User ID
	// and the value is the already assigned task count
	private final Map<String, Integer> userAssignedTaskCount = new HashMap<>();
	private final List<String> connectedUsers;

	public TaskUserAssignments(List<String> connectedUsers) {
		this.connectedUsers = connectedUsers;

		for (String u : connectedUsers) {
			userLimitOfTasks.put(u, 0);
			userAssignedTaskCount.put(u, 0);
		}

		// Initialize the max number of task and assigned task count for all connected
		// users
		initializeUserLimitOfTasksAndAssignedTaskCount();
	}

	/**
	 * Returns the limit of tasks and the assigned task count for all connected
	 * users
	 * 
	 * @return Object[3]: 0-User ID, 1-Limit, 2-task assigned
	 */

	private List<Object[]> getUserLimitAndAssignedTaskCountWithNoRefInventory() {
		StringBuilder hqlQuery = new StringBuilder();
		hqlQuery.append(" select u.id, ");
		hqlQuery.append("   obawolb_get_preference_value('OBAWO_LimitOfTaskForMobile', 'Y', null,");
		hqlQuery.append("   null, u.id, u.defaultRole.id, null) as task_limit, ");
		hqlQuery.append(
				"   COALESCE((select count(*) from OBAWO_Task task where task.userContact.id=u.id and task.status='AV'"
						+ " and  task.expectedMRefinventory is null group by u.id), 0)");
		hqlQuery.append(" from ADUser u ");
		hqlQuery.append(" where u.id IN (:userIds)");
		hqlQuery.append(" order by task_limit asc");

		final Query<Object[]> query = OBDal.getInstance().getSession().createQuery(hqlQuery.toString(), Object[].class);
		query.setParameterList("userIds", connectedUsers);
		return query.list();
	}

	private List<Object[]> getUserAssignedTaskWithRefInventory() {
		StringBuilder hqlQuery = new StringBuilder();
		hqlQuery.append("select distinct task.userContact.id, task.expectedMRefinventory.id from OBAWO_Task task");
		hqlQuery.append(" where task.status='AV'");
		hqlQuery.append(" and task.active='Y'");
		hqlQuery.append(" and task.expectedMRefinventory is not null ");
		hqlQuery.append(" and task.userContact.id IN (:userIds)");
		hqlQuery.append(" group by task.userContact.id, task.expectedMRefinventory.id");

		final Query<Object[]> query = OBDal.getInstance().getSession().createQuery(hqlQuery.toString(), Object[].class);
		query.setParameterList("userIds", connectedUsers);
		return query.list();
	}

	/**
	 * Initialize the Map with the limit of task and assigned task count per user
	 */
	private void initializeUserLimitOfTasksAndAssignedTaskCount() {
		for (Object[] p : getUserLimitAndAssignedTaskCountWithNoRefInventory()) {
			final Integer limit = Integer.parseInt(p[1].toString());
			final Integer assign = Integer.parseInt(p[2].toString());
			userLimitOfTasks.put(p[0].toString(), limit);
			userAssignedTaskCount.put(p[0].toString(), assign);
		}

		// Esto se ha hablado con Miguel Ángel y ahora el límite en vez de la
		// preferencia lo pone el puesto de trabajo
		for (Object[] p : getUserLimitByJobPosition()) {
			final Integer limit = Integer.parseInt(p[1].toString());
			userLimitOfTasks.put(p[0].toString(), limit);
		}

		// Increment assigned tasks that have referenced inventory
		for (Object[] p : getUserAssignedTaskWithRefInventory()) {
			final String userId = p[0].toString();
			incrementUserAssignedTaskCount(userId);
		}

	}

	private List<Object[]> getUserLimitByJobPosition() {
		StringBuilder hqlQuery = new StringBuilder();
		hqlQuery.append(" select u.id, coalesce(puestoTrabajo.limiteTareas, 1) as task_limit ");
		hqlQuery.append(" from ADUser u left join u.opxdesPuestoTrabajo puestoTrabajo ");
		hqlQuery.append(" where u.id IN (:userIds)");
		hqlQuery.append(" order by task_limit asc");

		final Query<Object[]> query = OBDal.getInstance().getSession().createQuery(hqlQuery.toString(), Object[].class);
		query.setParameterList("userIds", connectedUsers);
		return query.list();
	}

	/**
	 * Increments the assigned task count to the specified user
	 * 
	 * @param userId The user ID
	 */
	public void incrementUserAssignedTaskCount(String userId) {
		userAssignedTaskCount.put(userId, userAssignedTaskCount.get(userId) + 1);
	}

	/**
	 * Returns if the user is saturated or not.
	 * 
	 * @param userId The user ID.
	 * @return True if the assigned task count is equal to the limit of task
	 */
	public boolean isUserSaturated(String userId) {
		return userAssignedTaskCount.get(userId) >= userLimitOfTasks.get(userId);
	}

	/**
	 * Get the user with less assigned tasks. If all users are saturated return
	 * null.
	 * 
	 * @param userList
	 * @return the user with less assigned tasks
	 */
	public User getUserWithLessAssignedTasks(List<User> userList) {

		List<User> noSaturatedUserList = getNoSaturated(userList);
		if (noSaturatedUserList.isEmpty()) {
			return null;
		}

		return Collections.min(noSaturatedUserList, new Comparator<User>() {
			@Override
			public int compare(User u1, User u2) {
				return userAssignedTaskCount.get(u1.getId()) - userAssignedTaskCount.get(u2.getId());
			}
		});
	}

	private List<User> getNoSaturated(List<User> userFromRuleList) {
		List<User> list = new ArrayList<User>();
		for (User u : userFromRuleList) {
			if (!isUserSaturated(u.getId())) {
				list.add(u);
			}
		}
		return list;
	}

}
