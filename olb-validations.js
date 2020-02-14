OB.OBAWOLB = OB.OBAWOLB || {};
OB.OBAWOLB.ClientSideEventHandlers = {}
OB.OBAWOLB.USER_HEADER_TAB = '73E52D2888FC46D48607A3BFF3972976';
OB.OBAWOLB.ClientSideEventHandlers.validate = function (view, form, grid, extraParameters, actions) {
  var data = extraParameters.data,
  	  wilcardPattern = /(^%?[\s-\w]+%?$)|(%$)/,
  	  codePattern = /^[<>=]? *\d+$/;
 
  if (data.itt && !wilcardPattern.test(data.itt)) {
    view.messageBar.setMessage(isc.OBMessageBar.TYPE_ERROR, 'Invalid filter ', 'The Inv transaction type filter ' + data.itt + ' is not valid');
    return; // Interrupting save action: not calling OB.EventHandlerRegistry.callbackExecutor
  }
  if (data.allowFilterByUser){
	  if (!data.position || !wilcardPattern.test(data.position)){
		  view.messageBar.setMessage(isc.OBMessageBar.TYPE_ERROR, 'Invalid filter ', 'The operator filter ' + data.operator + ' is not valid');
		  return; // Interrupting save action: not calling OB.EventHandlerRegistry.callbackExecutor
	  }
  }
//  else if (!data.userContact){
//		  view.messageBar.setMessage(isc.OBMessageBar.TYPE_ERROR, 'Invalid operator', 'The operator can not be null');
//		  return; // Interrupting save action: not calling OB.EventHandlerRegistry.callbackExecutor
//	  }
  if (data.travelsequence && !wilcardPattern.test(data.travelsequence)) {
	    view.messageBar.setMessage(isc.OBMessageBar.TYPE_ERROR, 'Invalid filter ', 'The travel sequence filter ' + data.travelsequence + ' is not valid');
	    return; // Interrupting save action: not calling OB.EventHandlerRegistry.callbackExecutor
  }	  
	  
  if (data.popularityCode && !codePattern.test(data.popularityCode)) {
	    view.messageBar.setMessage(isc.OBMessageBar.TYPE_ERROR, 'Invalid filter ', 'The popularity code filter ' + data.popularityCode + ' is not valid');
	    return; // Interrupting save action: not calling OB.EventHandlerRegistry.callbackExecutor
 }	  
 OB.EventHandlerRegistry.callbackExecutor(view, form, grid, extraParameters, actions);
}; 
OB.EventHandlerRegistry.register(OB.OBAWOLB.USER_HEADER_TAB, OB.EventHandlerRegistry.PRESAVE, OB.OBAWOLB.ClientSideEventHandlers.validate, 'OBAWOLB_Validate');