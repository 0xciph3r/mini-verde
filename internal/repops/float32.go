// Package repops contains the explicitly rounded float32 operations used by
// the canonical machine. Keeping them small and unsurprising makes the
// numerical contract visible at every call site.
package repops

import "math"

// Add rounds an addition to float32 precision.
func Add(left, right float32) float32 {
	return float32(left + right)
}

// Sub rounds a subtraction to float32 precision.
func Sub(left, right float32) float32 {
	return float32(left - right)
}

// Mul rounds a product to float32 precision. The explicit conversion is a
// semantic FMA fence under the Go specification.
func Mul(left, right float32) float32 {
	return float32(left * right)
}

// Div rounds a quotient to float32 precision.
func Div(numerator, denominator float32) float32 {
	return float32(numerator / denominator)
}

// MulAdd rounds the product before adding it to the accumulator, then rounds
// the sum. Its result therefore cannot depend on fused multiply-add support.
func MulAdd(accumulator, left, right float32) float32 {
	product := float32(left * right)
	return float32(accumulator + product)
}

// SGD applies the protocol's three explicitly rounded update operations.
func SGD(value, gradientSum, batchSize, learningRate float32) float32 {
	gradient := float32(gradientSum / batchSize)
	scaled := float32(learningRate * gradient)
	return float32(value - scaled)
}

// IsFinite reports whether value is neither NaN nor an infinity.
func IsFinite(value float32) bool {
	converted := float64(value)
	return !math.IsNaN(converted) && !math.IsInf(converted, 0)
}
