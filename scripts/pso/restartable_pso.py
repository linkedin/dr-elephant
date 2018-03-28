import collections

import copy
import inspyred
from inspyred.ec import Bounder, Individual


class restartable_pso(inspyred.ec.EvolutionaryComputation):
    """Represents a basic particle swarm optimization algorithm.

    This class is built upon the ``EvolutionaryComputation`` class making
    use of an external archive and maintaining the population at the previous
    timestep, rather than a velocity. This approach was outlined in
    (Deb and Padhye, "Development of Efficient Particle Swarm Optimizers by
    Using Concepts from Evolutionary Algorithms", GECCO 2010, pp. 55--62).
    This class assumes that each candidate solution is a ``Sequence`` of
    real values.

    Public Attributes:

    - *topology* -- the neighborhood topology (default 
    topologies.star_topology)

    Optional keyword arguments in ``evolve`` args parameter:

    - *inertia* -- the inertia constant to be used in the particle
      updating (default 0.5)
    - *cognitive_rate* -- the rate at which the particle's current
      position influences its movement (default 2.1)
    - *social_rate* -- the rate at which the particle's neighbors
      influence its movement (default 2.1)

    """

    def __init__(self, random, _archive=[], _previous_population=[]):
        inspyred.ec.EvolutionaryComputation.__init__(self, random)
        self.topology = inspyred.swarm.topologies.star_topology
        self.selector = self._swarm_selector
        self.replacer = self._swarm_replacer
        self.variator = self._swarm_variator
        self.archiver = self._swarm_archiver

        self.archive = _archive
        self._previous_population = _previous_population

    def _swarm_archiver(self, random, population, archive, args):
        if len(archive) == 0:
            return population[:]
        else:
            new_archive = []
            for i, (p, a) in enumerate(zip(population[:], archive[:])):
                if p < a:
                    new_archive.append(a)
                else:
                    new_archive.append(p)
            return new_archive

    def _swarm_variator(self, random, candidates, args):
        inertia = args.setdefault('inertia', 0.5)
        cognitive_rate = args.setdefault('cognitive_rate', 2.1)
        social_rate = args.setdefault('social_rate', 2.1)
        if len(self.archive) == 0:
            self.archive = self.population[:]
        if len(self._previous_population) == 0:
            self._previous_population = self.population[:]

        neighbors = self.topology(self._random, self.archive, args)
        offspring = []
        for x, xprev, pbest, hood in zip(self.population,
                                         self._previous_population,
                                         self.archive, neighbors):
            nbest = max(hood)
            particle = []
            for xi, xpi, pbi, nbi in zip(x.candidate, xprev.candidate,
                                         pbest.candidate, nbest.candidate):
                value = (xi + inertia * (
                    xi - xpi) + cognitive_rate * random.random() * (
                             pbi - xi) + social_rate * random.random() * (
                             nbi - xi))
                particle.append(value)
            particle = self.bounder(particle, args)
            offspring.append(particle)
        return offspring

    def _swarm_selector(self, random, population, args):
        return population

    def _swarm_replacer(self, random, population, parents, offspring, args):
        self._previous_population = population[:]
        return offspring

    def evolve(self, generator, evaluator, pop_size=100, seeds=None, initial_fit=None,
               maximize=True, bounder=None, **args):
        """Perform the evolution.

        This function creates a population and then runs it through a series
        of evolutionary epochs until the terminator is satisfied. The general
        outline of an epoch is selection, variation, evaluation, replacement,
        migration, archival, and observation. The function returns a list of
        elements of type ``Individual`` representing the individuals contained
        in the final population.

        Arguments:

        - *generator* -- the function to be used to generate candidate 
        solutions
        - *evaluator* -- the function to be used to evaluate candidate 
        solutions
        - *pop_size* -- the number of Individuals in the population (default
        100)
        - *seeds* -- an iterable collection of candidate solutions to include
          in the initial population (default None)
        - *maximize* -- Boolean value stating use of maximization (default 
        True)
        - *bounder* -- a function used to bound candidate solutions (default
        None)
        - *args* -- a dictionary of keyword arguments

        The *bounder* parameter, if left as ``None``, will be initialized to a
        default ``Bounder`` object that performs no bounding on candidates.
        Note that the *_kwargs* class variable will be initialized to the 
        *args*
        parameter here. It will also be modified to include the following
        'built-in'
        keyword argument:

        - *_ec* -- the evolutionary computation (this object)

        """
        self._kwargs = args
        self._kwargs['_ec'] = self

        if seeds is None:
            seeds = []
        if bounder is None:
            bounder = Bounder()

        self.termination_cause = None
        self.generator = generator
        self.evaluator = evaluator
        self.bounder = bounder
        self.maximize = maximize
        self.population = []
        # self.archive = []

        # Create the initial population.
        if not isinstance(seeds, collections.Sequence):
            seeds = [seeds]
        initial_cs = copy.copy(seeds)
        num_generated = max(pop_size - len(seeds), 0)
        i = 0
        self.logger.debug('generating initial population')
        while i < num_generated:
            cs = generator(random=self._random, args=self._kwargs)
            initial_cs.append(cs)
            i += 1
        self.logger.debug('evaluating initial population')
        if initial_fit is None:
            initial_fit = evaluator(candidates=initial_cs, args=self._kwargs)

        for cs, fit in zip(initial_cs, initial_fit):
            if fit is not None:
                ind = Individual(cs, maximize=maximize)
                ind.fitness = fit
                self.population.append(ind)
            else:
                self.logger.warning(
                    'excluding candidate {0} because fitness received as '
                    'None'.format(cs))
        self.logger.debug(
            'population size is now {0}'.format(len(self.population)))

        self.num_evaluations = len(initial_fit)
        self.num_generations = 0

        self.logger.debug('archiving initial population')
        self.archive = self.archiver(random=self._random,
                                     population=list(self.population),
                                     archive=list(self.archive),
                                     args=self._kwargs)
        self.logger.debug('archive size is now {0}'.format(len(self.archive)))
        self.logger.debug(
            'population size is now {0}'.format(len(self.population)))

        if isinstance(self.observer, collections.Iterable):
            for obs in self.observer:
                self.logger.debug(
                    'observation using {0} at generation {1} and '
                    'evaluation {'
                    '2}'.format(obs.__name__, self.num_generations,
                                self.num_evaluations))
                obs(population=list(self.population),
                    num_generations=self.num_generations,
                    num_evaluations=self.num_evaluations, args=self._kwargs)
        else:
            self.logger.debug(
                'observation using {0} at generation {1} and evaluation {'
                '2}'.format(self.observer.__name__, self.num_generations,
                            self.num_evaluations))
            self.observer(population=list(self.population),
                          num_generations=self.num_generations,
                          num_evaluations=self.num_evaluations,
                          args=self._kwargs)

        while not self._should_terminate(list(self.population),
                                         self.num_generations,
                                         self.num_evaluations):
            # Select individuals.
            self.logger.debug(
                'selection using {0} at generation {1} and evaluation {'
                '2}'.format(self.selector.__name__, self.num_generations,
                            self.num_evaluations))
            parents = self.selector(random=self._random,
                                    population=list(self.population),
                                    args=self._kwargs)
            self.logger.debug('selected {0} candidates'.format(len(parents)))
            parent_cs = [copy.deepcopy(i.candidate) for i in parents]
            offspring_cs = parent_cs

            if isinstance(self.variator, collections.Iterable):
                for op in self.variator:
                    self.logger.debug(
                        'variation using {0} at generation {1} and '
                        'evaluation '
                        '{2}'.format(op.__name__, self.num_generations,
                                     self.num_evaluations))
                    offspring_cs = op(random=self._random,
                                      candidates=offspring_cs,
                                      args=self._kwargs)
            else:
                self.logger.debug(
                    'variation using {0} at generation {1} and evaluation '
                    '{2}'.format(self.variator.__name__,
                                 self.num_generations,
                                 self.num_evaluations))
                offspring_cs = self.variator(random=self._random,
                                             candidates=offspring_cs,
                                             args=self._kwargs)
            self.logger.debug(
                'created {0} offspring'.format(len(offspring_cs)))

            # Evaluate offspring.
            self.logger.debug(
                'evaluation using {0} at generation {1} and evaluation {'
                '2}'.format(evaluator.__name__, self.num_generations,
                            self.num_evaluations))
            offspring_fit = evaluator(candidates=offspring_cs,
                                      args=self._kwargs)
            offspring = []
            for cs, fit in zip(offspring_cs, offspring_fit):
                if fit is not None:
                    off = Individual(cs, maximize=maximize)
                    off.fitness = fit
                    offspring.append(off)
                else:
                    self.logger.warning(
                        'excluding candidate {0} because fitness '
                        'received as '
                        'None'.format(cs))
            self.num_evaluations += len(offspring_fit)

            # Replace individuals.
            self.logger.debug(
                'replacement using {0} at generation {1} and evaluation {'
                '2}'.format(self.replacer.__name__, self.num_generations,
                            self.num_evaluations))
            self.population = self.replacer(random=self._random,
                                            population=self.population,
                                            parents=parents,
                                            offspring=offspring,
                                            args=self._kwargs)
            self.logger.debug(
                'population size is now {0}'.format(len(self.population)))

            # Migrate individuals.
            self.logger.debug(
                'migration using {0} at generation {1} and evaluation {'
                '2}'.format(self.migrator.__name__, self.num_generations,
                            self.num_evaluations))
            self.population = self.migrator(random=self._random,
                                            population=self.population,
                                            args=self._kwargs)
            self.logger.debug(
                'population size is now {0}'.format(len(self.population)))

            # Archive individuals.
            self.logger.debug(
                'archival using {0} at generation {1} and evaluation {'
                '2}'.format(self.archiver.__name__, self.num_generations,
                            self.num_evaluations))
            # self.archive = self.archiver(random=self._random,
            #                              archive=self.archive,
            #                              population=list(self.population),
            #                              args=self._kwargs)
            self.logger.debug(
                'archive size is now {0}'.format(len(self.archive)))
            self.logger.debug(
                'population size is now {0}'.format(len(self.population)))

            self.num_generations += 1
            if isinstance(self.observer, collections.Iterable):
                for obs in self.observer:
                    self.logger.debug(
                        'observation using {0} at generation {1} and '
                        'evaluation {2}'.format(obs.__name__,
                                                self.num_generations,
                                                self.num_evaluations))
                    obs(population=list(self.population),
                        num_generations=self.num_generations,
                        num_evaluations=self.num_evaluations,
                        args=self._kwargs)
            else:
                self.logger.debug(
                    'observation using {0} at generation {1} and '
                    'evaluation {2}'.format(self.observer.__name__,
                                            self.num_generations,
                                            self.num_evaluations))
                self.observer(population=list(self.population),
                              num_generations=self.num_generations,
                              num_evaluations=self.num_evaluations,
                              args=self._kwargs)

        return self.population
