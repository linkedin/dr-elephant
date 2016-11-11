export function initialize(application) {
  application.inject('controller', 'notifications', 'service:notification-messages');
}

export default {
  name: 'inject-notifications',
  initialize: initialize
};
