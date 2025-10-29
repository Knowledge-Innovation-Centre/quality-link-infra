import { createContext, useContext, useState, ReactNode, useCallback, useEffect } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import { CheckCircle, XCircle, AlertCircle, Info, X, Loader2 } from 'lucide-react'

type ToastType = 'success' | 'error' | 'warning' | 'info' | 'loading'

interface Toast {
  id: string
  type: ToastType
  title: string
  message?: string
  duration?: number
  progress?: number
  showProgress?: boolean
  isLoading?: boolean
  isComplete?: boolean
  onClose?: () => void
  changes?: Array<{
    id: string
    name: string
    oldValue: number
    newValue: number
  }>
}

interface ToastContextType {
  showToast: (toast: Omit<Toast, 'id'>) => string
  hideToast: (id: string) => void
  updateToast: (id: string, updates: Partial<Omit<Toast, 'id'>>) => void
}

const ToastContext = createContext<ToastContextType | undefined>(undefined)

export function ToastProvider({ children }: { children: ReactNode }) {
  const [toasts, setToasts] = useState<Toast[]>([])

  const showToast = useCallback((toast: Omit<Toast, 'id'>) => {
    const id = Math.random().toString(36).substr(2, 9)
    const newToast = { ...toast, id }
    setToasts((prev) => [...prev, newToast])

    // Auto remove after duration (unless it's a loading toast)
    if (toast.type !== 'loading' && !toast.isLoading) {
      const duration = toast.duration || 5000
      setTimeout(() => {
        hideToast(id)
      }, duration)
    }
    
    return id
  }, [])

  const hideToast = useCallback((id: string) => {
    setToasts((prev) => prev.filter((toast) => toast.id !== id))
  }, [])

  const updateToast = useCallback((id: string, updates: Partial<Omit<Toast, 'id'>>) => {
    setToasts((prev) =>
      prev.map((toast) => (toast.id === id ? { ...toast, ...updates } : toast))
    )
    
    // If updating to a non-loading type, set auto-dismiss
    if (updates.type && updates.type !== 'loading' && !updates.isLoading) {
      const duration = updates.duration || 5000
      setTimeout(() => {
        hideToast(id)
      }, duration)
    }
  }, [])

  return (
    <ToastContext.Provider value={{ showToast, hideToast, updateToast }}>
      {children}
      <ToastContainer toasts={toasts} onClose={hideToast} />
    </ToastContext.Provider>
  )
}

export function useToast() {
  const context = useContext(ToastContext)
  if (!context) {
    throw new Error('useToast must be used within ToastProvider')
  }
  return context
}

interface ToastContainerProps {
  toasts: Toast[]
  onClose: (id: string) => void
}

function ToastContainer({ toasts, onClose }: ToastContainerProps) {
  return (
    <div className="fixed top-4 right-4 z-50 space-y-3 max-w-md">
      <AnimatePresence>
        {toasts.map((toast) => (
          <ToastItem key={toast.id} toast={toast} onClose={onClose} />
        ))}
      </AnimatePresence>
    </div>
  )
}

interface ToastItemProps {
  toast: Toast
  onClose: (id: string) => void
}

const toastStyles = {
  success: {
    bg: 'bg-turquoise-100',
    border: 'border-turquoise-200',
    text: 'text-turquoise-600',
    icon: CheckCircle,
  },
  error: {
    bg: 'bg-red-50',
    border: 'border-red-50',
    text: 'text-red-800',
    icon: XCircle,
  },
  warning: {
    bg: 'bg-turquoise-100',
    border: 'border-turquoise-200',
    text: 'text-turquoise-base',
    icon: AlertCircle,
  },
  info: {
    bg: 'bg-turquoise-100',
    border: 'border-turquoise-200',
    text: 'text-turquoise-600',
    icon: Info,
  },
  loading: {
    bg: 'bg-white',
    border: 'border-gray-200',
    text: 'text-gray-900',
    icon: Loader2,
  },
}

function ToastItem({ toast, onClose }: ToastItemProps) {
  const style = toastStyles[toast.type]
  const Icon = style.icon
  const [internalProgress, setInternalProgress] = useState(toast.progress || 0)

  // Update progress when toast.progress changes
  useEffect(() => {
    if (toast.progress !== undefined) {
      setInternalProgress(toast.progress)
    }
  }, [toast.progress])

  // Detailed loading/success toast (transforms from loading to success)
  if (toast.isLoading || toast.type === 'loading' || toast.isComplete) {
    return (
      <motion.div
        initial={{ opacity: 0, x: 100, scale: 0.9 }}
        animate={{ opacity: 1, x: 0, scale: 1 }}
        exit={{ opacity: 0, x: 100, scale: 0.9 }}
        transition={{ type: 'spring', duration: 0.3 }}
        className="bg-white border border-gray-200 rounded-lg shadow-lg p-4 min-w-[357px] max-w-[357px]"
      >
        {/* Header with spinner/checkmark and text */}
        <div className="flex items-center gap-4 mb-4">
          {toast.isComplete ? (
            <div className="w-[50px] h-[50px] bg-brand-base rounded-full flex items-center justify-center relative">
              <CheckCircle className="w-4 h-4 text-white" />
              <div className="absolute inset-0 border-[3px] border-[#90cee9] rounded-full" />
            </div>
          ) : (
            <div className="w-[50px] h-[50px] flex items-center justify-center">
              <Loader2 className="w-[50px] h-[50px] text-brand-base animate-spin" />
            </div>
          )}
          <div className="flex-1">
            <p className="text-sm font-bold text-gray-900">{toast.title}</p>
            {toast.message && (
              <p className="text-sm font-medium text-gray-500 mt-1">
                {toast.message}
              </p>
            )}
          </div>
        </div>

        {/* Progress bar (only show during loading) */}
        {!toast.isComplete && toast.showProgress && (
          <div className="mb-4">
            <div className="flex items-center justify-between mb-1">
              <span className="text-xs font-medium text-gray-500">
                {Math.round(internalProgress)}%
              </span>
            </div>
            <div className="w-full h-1.5 bg-gray-200 rounded-sm relative overflow-hidden">
              <motion.div
                className="absolute left-0 top-0 h-full bg-brand-base rounded-sm"
                initial={{ width: 0 }}
                animate={{ width: `${internalProgress}%` }}
                transition={{ duration: 0.3 }}
              />
            </div>
          </div>
        )}

        {/* Changes list (only show when complete) */}
        {toast.isComplete && toast.changes && (
          <>
            <div className="border-t border-gray-200 mb-4" />
            <p className="text-sm font-semibold text-gray-500 mb-4">Changes found:</p>
            <div className="space-y-2 mb-4">
              {toast.changes.map((change, index) => (
                <div key={index}>
                  <div className="space-y-2">
                    <div className="flex gap-1">
                      <span className="text-sm font-semibold text-gray-900 w-[135px]">ID</span>
                      <span className="text-sm font-medium text-gray-900">{change.id}</span>
                    </div>
                    <div className="flex gap-1">
                      <span className="text-sm font-semibold text-gray-900 w-[135px]">NAME</span>
                      <span className="text-sm font-medium text-gray-900 flex-1 truncate">{change.name}</span>
                    </div>
                    <div className="flex gap-1 items-center">
                      <span className="text-sm font-semibold text-gray-900 w-[135px]"># OF LEARNING OPPORTUNITIES</span>
                      <div className="flex items-center gap-2">
                        <span className="text-sm font-medium text-gray-500">{change.oldValue}</span>
                        <svg className="w-3 h-3 text-gray-400 rotate-90" fill="currentColor" viewBox="0 0 12 12">
                          <path d="M6 0L4.5 1.5L8.25 5.25H0V6.75H8.25L4.5 10.5L6 12L12 6L6 0Z"/>
                        </svg>
                        <span className="text-sm font-bold text-brand-base">{change.newValue}</span>
                      </div>
                    </div>
                  </div>
                  {index < (toast.changes?.length ?? 0) - 1 && (
                    <div className="border-t border-gray-200 my-2" />
                  )}
                </div>
              ))}
            </div>
          </>
        )}

        {/* Separator */}
        <div className="border-t border-gray-200 mb-4" />

        {/* Close button */}
        <button
          onClick={() => {
            toast.onClose?.()
            onClose(toast.id)
          }}
          disabled={!toast.isComplete && toast.isLoading}
          className="w-full bg-brand-base text-white px-5 py-2.5 rounded-lg text-sm font-medium transition-opacity disabled:opacity-30 hover:opacity-90"
        >
          Close
        </button>
      </motion.div>
    )
  }

  // Standard toast
  return (
    <motion.div
      initial={{ opacity: 0, x: 100, scale: 0.9 }}
      animate={{ opacity: 1, x: 0, scale: 1 }}
      exit={{ opacity: 0, x: 100, scale: 0.9 }}
      transition={{ type: 'spring', duration: 0.3 }}
      className={`
        ${style.bg} ${style.border} border rounded-lg shadow-lg p-4
        flex items-start gap-3 min-w-[320px]
      `}
    >
      <Icon className={`w-5 h-5 ${style.text} flex-shrink-0 mt-0.5`} />
      <div className="flex-1 min-w-0">
        <p className={`font-semibold ${style.text}`}>{toast.title}</p>
        {toast.message && (
          <p className={`text-sm ${style.text} mt-1 opacity-90`}>
            {toast.message}
          </p>
        )}
      </div>
      <button
        onClick={() => onClose(toast.id)}
        className={`${style.text} hover:opacity-70 transition-opacity flex-shrink-0`}
        aria-label="Close notification"
      >
        <X className="w-4 h-4" />
      </button>
    </motion.div>
  )
}

