import * as winston from 'winston';

const { combine, simple, colorize } = winston.format;

const outputLoggers = [
  new winston.transports.Console({
    level: process.env.LOGGING_LEVEL || 'info',
    format: combine(colorize(), simple()),
  }),
];

const loggerLevels = {
  levels: {
    trace: 0,
    input: 1,
    verbose: 2,
    prompt: 3,
    debug: 4,
    info: 5,
    data: 6,
    help: 7,
    warn: 8,
    error: 9,
  },
  colors: {
    trace: 'magenta',
    input: 'grey',
    verbose: 'cyan',
    prompt: 'grey',
    debug: 'blue',
    info: 'green',
    data: 'grey',
    help: 'cyan',
    warn: 'yellow',
    error: 'red',
  },
};

winston.addColors(loggerLevels.colors);

const Logger = winston.createLogger({
  level: process.env.LOGGING_LEVEL || 'info',
  levels: loggerLevels.levels,
  transports: outputLoggers,
});

export default Logger;
