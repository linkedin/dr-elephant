Temporary readme file. I will remove this after the code is reviewed!

This link might be useful in understand the Exceptions structure:
https://iwww.corp.linkedin.com/wiki/cf/display/ENGS/Logging+Event+Schema

EventException: Captures an exception event (A part of exception chain)
                For example:

EventExceptionToAvro: Converts the EventException java object to json format.
                      For example:

ExceptionLoggingEvent: A class for a logging event of an exception. Captures a chain of EventException.
                        For example:

EventExceptionToAvro: Converts the EventException java object to json format.
                      For example:

HadoopException: A list of ExceptionLoggingEvent. This class is made to avoid making three classes i.e. AzkabanException, ScriptException
and MRException AND TaskException separately.










Assumptions:
If there is a MR Level error, I assume that the script level error is because of that only and don't show it again
My input url parser can only parse the azban excution url and nothing else for now!
While analyzing a task failure, I just analyze the last attempt of that task







To do:
Exception finder, variable exception is used at 2 places and is confusing.