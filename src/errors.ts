/**
 * @fileoverview Custom error class for CollegeDB operations
 *
 * This module provides the CollegeDBError class that extends the native Error class
 * to provide more specific error information for CollegeDB operations. This allows
 * for better error handling and debugging throughout the application.
 *
 * @example
 * ```typescript
 * import { CollegeDBError } from './errors.js';
 *
 * throw new CollegeDBError('Failed to allocate shard', 'SHARD_ALLOCATION_ERROR');
 * ```
 *
 * @author CollegeDB Team
 * @since 1.0.2
 */

/**
 * Custom error class for CollegeDB operations
 *
 * Extends the native Error class to provide more specific error information
 * for CollegeDB operations. Includes an optional error code for better
 * error categorization and handling.
 *
 * @example
 * ```typescript
 * try {
 *   await getShardForKey('invalid-key');
 * } catch (error) {
 *   if (error instanceof CollegeDBError) {
 *     console.error(`CollegeDB Error (${error.code}): ${error.message}`);
 *   }
 * }
 * ```
 */
export class CollegeDBError extends Error {
	/**
	 * Optional error code for categorizing different types of errors
	 */
	public readonly code?: string;

	/**
	 * Creates a new CollegeDBError instance
	 * @param message - Error message describing what went wrong
	 * @param code - Optional error code for categorization
	 * @example
	 * ```typescript
	 * throw new CollegeDBError('Shard not found', 'SHARD_NOT_FOUND');
	 * ```
	 */
	constructor(message: string, code?: string) {
		super(message);
		this.name = 'CollegeDBError';
		this.code = code;

		// Maintains proper stack trace for where our error was thrown (only available on V8)
		if (Error.captureStackTrace) {
			Error.captureStackTrace(this, CollegeDBError);
		}
	}
}
