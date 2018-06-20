# Copyright 2016 LinkedIn Corp.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

import inspyred
from random import Random
import argparse
import time
import json
import imp
import restartable_pso

param_value_range = []
param_step_size = []
param_default_value = []
param_name = []
iteration = 0
job_type = ""

LARGE_DUMMY_FITNESS = 10000

PARAM_PIG_MAX_COMBINED_SPLIT_SIZE = 'pig.maxCombinedSplitSize'
PARAM_MAPREDUCE_TASK_IO_SORT_FACTOR = 'mapreduce.task.io.sort.factor'
PARAM_MAPREDUCE_TASK_IO_SORT_MB = 'mapreduce.task.io.sort.mb'
PARAM_MAPREDUCE_MAP_SORT_SPILL_PERCENT = 'mapreduce.map.sort.spill.percent'
PARAM_MAPREDUCE_MAP_MEMORY_MB = 'mapreduce.map.memory.mb'
PARAM_MAPREDUCE_REDUCE_MEMORY_MB = 'mapreduce.reduce.memory.mb'
PARAM_MAPREDUCE_INPUT_FILEINPUTFORMAT_SPLIT_MAXSIZE = 'mapreduce.input.fileinputformat.split.maxsize'
PARAM_MAPREDUCE_MAP_JAVA_OPTS = 'mapreduce.map.java.opts'
PARAM_MAPREDUCE_REDUCE_JAVA_OPTS = 'mapreduce.reduce.java.opts'

ARG_TUNING_STATE_KEY = 'json_tuning_state'
ARG_PARAMETERS_TO_TUNE_KEY = 'parameters_to_tune'
ARG_JOB_TYPE = "job_type"

TUNING_STATE_ARCHIVE_KEY = 'archive'
TUNING_STATE_PREV_POPULATION_KEY = 'prev_population'
TUNING_STATE_CURRENT_POPULATION_KEY = 'current_population'
TUNING_STATE_RANDOM_STATE_KEY = 'rnd_state'

INDIVIDUAL_CANDIDATE_KEY = '_candidate'
INDIVIDUAL_MAXIMIZE_KEY = 'maximize'
INDIVIDUAL_FITNESS_KEY = 'fitness'
INDIVIDUAL_BIRTHDAY_KEY = 'birthdate'

PARAMETER_NAME_KEY = 'paramName'
PARAMETER_STEP_SIZE_KEY = 'stepSize'
PARAMETER_DEFAULT_VALUE_KEY = 'defaultValue'
PARAMETER_MAX_VALUE_KEY = 'maxValue'
PARAMETER_MIN_VALUE_KEY = 'minValue'

INITIAL_DERIVED_LOWER_MEMORY_PARAM_RANGE = (0.5, 0.8)
INITIAL_DERIVED_UPPER_MEMORY_PARAM_RANGE = (1.05, 1.1)
INITIAL_DERIVED_SORT_MEMORY_PARAM_RANGE = (0.0, 0.25)
# POPULATION_SIZE = 3 performs the best as was found in the experimentation
POPULATION_SIZE = 3


def initialize_params(parameters_to_tune):
    """Initializes data structures for generating new parameter suggestion
    :param parameters_to_tune: The list of parameters to be tuned in json format
    :return: None
    """
    for parameter in parameters_to_tune:
        name = str(parameter[PARAMETER_NAME_KEY])
        step_size = float(parameter[PARAMETER_STEP_SIZE_KEY])
        default_value = float(parameter[PARAMETER_DEFAULT_VALUE_KEY])
        max_value = float(parameter[PARAMETER_MAX_VALUE_KEY])
        min_value = float(parameter[PARAMETER_MIN_VALUE_KEY])
        param_name.append(name)
        param_default_value.append(default_value)
        param_step_size.append(step_size)
        param_value_range.append((min_value, max_value))


def initial_population_generator(random, args):
    """ Generates the initial population for a job new to auto tuning
    :param random:
    :param args:
    :return: initial population
    """

    for i in range(0, len(param_name)):
        if param_name[i] == PARAM_MAPREDUCE_TASK_IO_SORT_FACTOR:
            mr_sort_factor_index = i
        elif param_name[i] == PARAM_MAPREDUCE_TASK_IO_SORT_MB:
            mr_sort_memory_index = i
        elif param_name[i] == PARAM_MAPREDUCE_MAP_SORT_SPILL_PERCENT:
            mr_spill_percent_index = i
        elif param_name[i] == PARAM_MAPREDUCE_MAP_MEMORY_MB:
            mr_map_memory_index = i
        elif param_name[i] == PARAM_MAPREDUCE_REDUCE_MEMORY_MB:
            mr_reduce_memory_index = i
        elif param_name[i] == PARAM_PIG_MAX_COMBINED_SPLIT_SIZE:
            pig_max_combined_split_size_index = i
    global iteration

    if iteration == 0:
        iteration += 1
        initial_population = param_default_value

    else:
        initial_population = [random.uniform(x, y) for x, y in param_value_range]

        if job_type == 'PIG':
            if iteration % 2 == 1:
                initial_population[mr_map_memory_index] = random.uniform(INITIAL_DERIVED_LOWER_MEMORY_PARAM_RANGE[0],
                                                                         INITIAL_DERIVED_LOWER_MEMORY_PARAM_RANGE[1]) * \
                                                          param_default_value[mr_map_memory_index]
                initial_population[mr_reduce_memory_index] = random.uniform(INITIAL_DERIVED_LOWER_MEMORY_PARAM_RANGE[0],
                                                                            INITIAL_DERIVED_LOWER_MEMORY_PARAM_RANGE[
                                                                                1]) * param_default_value[
                                                                 mr_reduce_memory_index]

            if iteration % 2 == 0:
                initial_population[mr_map_memory_index] = random.uniform(INITIAL_DERIVED_UPPER_MEMORY_PARAM_RANGE[0],
                                                                         INITIAL_DERIVED_UPPER_MEMORY_PARAM_RANGE[1]) * \
                                                          param_default_value[mr_map_memory_index]
                initial_population[mr_reduce_memory_index] = random.uniform(INITIAL_DERIVED_UPPER_MEMORY_PARAM_RANGE[0],
                                                                            INITIAL_DERIVED_UPPER_MEMORY_PARAM_RANGE[
                                                                                1]) * param_default_value[
                                                                 mr_reduce_memory_index]

            initial_population[mr_sort_memory_index] = random.uniform(INITIAL_DERIVED_SORT_MEMORY_PARAM_RANGE[0],
                                                                      INITIAL_DERIVED_SORT_MEMORY_PARAM_RANGE[1]) * \
                                                       initial_population[mr_map_memory_index]
            initial_population[pig_max_combined_split_size_index] = param_default_value[
                pig_max_combined_split_size_index]
            iteration += 1

    for i in range(0, len(param_name)):
        (min_val, max_val) = param_value_range[i]
        step = param_step_size[i]
        initial_population[i] = int(round(initial_population[i] * 1.0 / step)) * step
        initial_population[i] = max(min_val, min(max_val, initial_population[i]))

    return initial_population


def get_params_lower_bound():
    """Returns the lower bound of param value range
    :return: list containing lower bound of each parameter
    """
    return [x for x, y in param_value_range]


def get_params_upper_bound():
    """Returns the upper bound of param value range
    :return: list containing upper bound of each parameter
    """
    return [y for x, y in param_value_range]


def bounder(candidate, args):
    """ Bounds the candidate (parameter values) within the provided range
    :param candidate: parameter value set
    :param args:
    :return: Bounded candidate (parameter values)
    """
    param_value_lower_bound = get_params_lower_bound()
    param_value_upper_bound = get_params_upper_bound()
    bounded_candidate = candidate
    for i, (c, lo, hi, step) in enumerate(
            zip(candidate, param_value_lower_bound, param_value_upper_bound, param_step_size)):
        c = int(round(c * 1.0 / step)) * step
        bounded_candidate[i] = max(min(c, hi), lo)
    return bounded_candidate


def dummy_fitness_evaluator(candidates, args):
    """ Assigns a temporary large fitness to each candidate which will be overwritten by cost function value
    when execution will be completed and execution metrics are available
    :param candidates: List of candidates whose dummy fitness it to be computed
    :param args:
    :return: Returns a list of dummy fitness values for each candidate
    """
    fitness = []
    for candidate in candidates:
        # Assign a temporary large dummy fitness to each candidate
        fitness.append(LARGE_DUMMY_FITNESS)
    return fitness


def generate_tuning_state(pso, pseudo_random_number_generator, population):
    """ Generates the tuning state from PSO object, current population and random number generator object
    :param pso: PSO object
    :param pseudo_random_number_generator: Random object
    :param population: Current population
    :return: Update tuning state in json format
    """
    data = {}
    archive = []
    prev_population = []
    current_population = []
    rnd_state = json.dumps(pseudo_random_number_generator.getstate())

    for individual in pso.archive:
        archive.append(individual.__dict__)
    for individual in pso._previous_population:
        prev_population.append(individual.__dict__)
    for individual in population:
        current_population.append(individual.__dict__)

    data[TUNING_STATE_ARCHIVE_KEY] = archive
    data[TUNING_STATE_PREV_POPULATION_KEY] = prev_population
    data[TUNING_STATE_CURRENT_POPULATION_KEY] = current_population
    data[TUNING_STATE_RANDOM_STATE_KEY] = rnd_state
    data_dump = json.dumps(data)
    return data_dump


def json_to_individual_object(json_list):
    """ Convert json data to list of inspyred.ec.ec.Individual objects
    :param json_list: list of individuals in json format
    :return: list of inspyred.ec.ec.Individual objects
    """
    individuals = []

    for element in json_list:
        individual = inspyred.ec.ec.Individual()
        individual.candidate = element[INDIVIDUAL_CANDIDATE_KEY]
        individual.maximize = element[INDIVIDUAL_MAXIMIZE_KEY]
        individual.fitness = element[INDIVIDUAL_FITNESS_KEY]
        individual.birthday = element[INDIVIDUAL_BIRTHDAY_KEY]
        individuals.append(individual)
    return individuals


def main(json_tuning_state, display=False):
    """Computes the new tuning state which contains the new population set and prints it in stdout
    so that it can be read by java code
    :param json_tuning_state: Current tuning state
    :param display:
    :return: None
    """
    tuning_state = json.loads(json_tuning_state)
    pseudo_random_number_generator = Random()
    args = {}

    if TUNING_STATE_ARCHIVE_KEY not in tuning_state:
        pseudo_random_number_generator.seed(time.time())
        pso = restartable_pso.restartable_pso(pseudo_random_number_generator)
        pso.observer = inspyred.ec.observers.default_observer
        pso.terminator = inspyred.ec.terminators.evaluation_termination
        pso.topology = inspyred.swarm.topologies.ring_topology
        population = pso.evolve(generator=initial_population_generator, evaluator=dummy_fitness_evaluator,
                                pop_size=POPULATION_SIZE,
                                bounder=bounder,
                                maximize=False, max_evaluations=POPULATION_SIZE, **args)

        tuning_state = generate_tuning_state(pso, pseudo_random_number_generator, population)
        print(tuning_state)

    else:
        archive = json_to_individual_object(tuning_state[TUNING_STATE_ARCHIVE_KEY])
        prev_population = json_to_individual_object(tuning_state[TUNING_STATE_PREV_POPULATION_KEY])
        initial_population = json_to_individual_object(tuning_state[TUNING_STATE_CURRENT_POPULATION_KEY])

        str_rnd_state = tuning_state[TUNING_STATE_RANDOM_STATE_KEY]
        json_rnd_state = json.loads(str_rnd_state)
        json_rnd_state[1] = tuple(json_rnd_state[1])
        rnd_state = tuple(json_rnd_state)

        pseudo_random_number_generator.setstate(rnd_state)

        pso = restartable_pso.restartable_pso(pseudo_random_number_generator, _archive=archive,
                                              _previous_population=prev_population)
        pso.observer = inspyred.ec.observers.default_observer
        pso.terminator = inspyred.ec.terminators.evaluation_termination
        pso.topology = inspyred.swarm.topologies.ring_topology

        population = pso.evolve(seeds=[cs.candidate for cs in initial_population],
                                initial_fit=[cs.fitness for cs in initial_population],
                                generator=None, evaluator=dummy_fitness_evaluator, pop_size=POPULATION_SIZE,
                                bounder=bounder,
                                maximize=False, max_evaluations=2 * POPULATION_SIZE, **args)
        tuning_state = generate_tuning_state(pso, pseudo_random_number_generator, population)
        print(tuning_state)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(ARG_TUNING_STATE_KEY, help='Saved tuning state object')
    parser.add_argument(ARG_PARAMETERS_TO_TUNE_KEY)
    parser.add_argument(ARG_JOB_TYPE)
    args = parser.parse_args()
    json_tuning_state = args.json_tuning_state
    parameters_to_tune = args.parameters_to_tune
    job_type = args.job_type
    parameters_to_tune = json.loads(parameters_to_tune)
    initialize_params(parameters_to_tune)
    main(json_tuning_state)
