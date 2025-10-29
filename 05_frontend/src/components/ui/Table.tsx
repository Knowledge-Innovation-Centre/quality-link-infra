import { ReactNode } from 'react'

interface TableProps {
  children: ReactNode
  className?: string
}

interface TableHeaderProps {
  children: ReactNode
}

interface TableBodyProps {
  children: ReactNode
}

interface TableRowProps {
  children: ReactNode
  className?: string
}

interface TableCellProps {
  children: ReactNode
  className?: string
  header?: boolean
}

export function Table({ children, className = '' }: TableProps) {
  return (
    <div className="overflow-x-auto">
      <table className={`w-full border-collapse ${className}`}>
        {children}
      </table>
    </div>
  )
}

export function TableHeader({ children }: TableHeaderProps) {
  return <thead className="bg-gray-50 border-b border-gray-200">{children}</thead>
}

export function TableBody({ children }: TableBodyProps) {
  return <tbody className="divide-y divide-gray-200">{children}</tbody>
}

export function TableRow({ children, className = '' }: TableRowProps) {
  return <tr className={`hover:bg-gray-50 ${className}`}>{children}</tr>
}

export function TableCell({ children, header = false, className = '' }: TableCellProps) {
  const Tag = header ? 'th' : 'td'
  const baseStyles = header
    ? 'px-4 py-3 text-left text-sm font-semibold text-gray-700'
    : 'px-4 py-3.5 text-sm text-gray-900'
  
  return (
    <Tag className={`${baseStyles} ${className}`}>
      {children}
    </Tag>
  )
}


