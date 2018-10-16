package com.linkedin.drelephant.tuning;

/**
 * This is the top most managers . All the implementation
 * of managers should implement this interface and expose execute method.
 * Execute method will be the core of the manager and will do the bulk of the work .
 * Execute will be exposed as public method and will be used to execute the functionality
 * of the respective manager
 */
public interface Manager {
  /**
   * Use to execute the logic of all the managers .
   * @return
   */
  boolean execute();

  /**
   * Used to get the manager name
   * @return
   */
  String getManagerName();

}
