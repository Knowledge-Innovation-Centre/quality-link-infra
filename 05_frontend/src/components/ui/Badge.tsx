import { ReactNode } from 'react'
import { BadgeVariant } from '@/types'

interface BadgeProps {
  variant?: BadgeVariant
  children: ReactNode
  className?: string
}

const variantStyles: Record<BadgeVariant, string> = {
  default: 'bg-turquoise-100 text-turquoise-900 border-turquoise-200',
  success: 'bg-turquoise-100 text-turquoise-600 border-turquoise-200',
  warning: 'bg-turquoise-base text-white border-turquoise-base',
  error: 'bg-red-50 text-red-800 border-red-50',
  info: 'bg-turquoise-100 text-turquoise-600 border-turquoise-200',
}

export default function Badge({
  variant = 'default',
  children,
  className = '',
}: BadgeProps) {
  return (
    <span
      className={`
        inline-flex items-center gap-1.5
        px-3 py-1 rounded-md
        text-sm font-medium
        border
        ${variantStyles[variant]}
        ${className}
      `}
    >
      {children}
    </span>
  )
}


